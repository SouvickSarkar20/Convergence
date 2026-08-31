import asyncio
# pyrefly: ignore [missing-import]
import websockets
import random
import subprocess
import hashlib
import sys
import os

# Workaround for Windows asyncio ProactorEventLoop bugs during socket serving
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROXY_PORT = 8081
SERVER_PORT = 8080
UPSTREAM_URL = f"ws://127.0.0.1:{SERVER_PORT}/ws"
PROXY_URL = f"ws://127.0.0.1:{PROXY_PORT}/ws"

active_tasks = []

async def send_delayed(target_ws, message, delay, direction):
    await asyncio.sleep(delay)
    try:
        await target_ws.send(message)
    except Exception:
        pass

async def forward_with_chaos(source_ws, target_ws, direction):
    try:
        async for message in source_ws:
            # 1. Simulate Packet Drops (0% chance - WebSockets are built on reliable TCP)
            if random.random() < 0.0:
                print(f"    [CHAOS] {direction} - Dropped packet")
                continue

            # 2. Simulate Latency Jitter (30% chance of introducing delay, between 50ms and 400ms)
            if random.random() < 0.30:
                delay = random.uniform(0.05, 0.40)
                task = asyncio.create_task(send_delayed(target_ws, message, delay, direction))
                active_tasks.append(task)
            else:
                # Send immediately
                try:
                    await target_ws.send(message)
                except Exception:
                    pass
    except websockets.exceptions.ConnectionClosed:
        pass

async def proxy_handler(client_ws):
    try:
        async with websockets.connect(UPSTREAM_URL) as server_ws:
            task_c2s = asyncio.create_task(forward_with_chaos(client_ws, server_ws, "Client -> Server"))
            task_s2c = asyncio.create_task(forward_with_chaos(server_ws, client_ws, "Server -> Client"))
            
            # Wait until one of the directions closes the connection
            done, pending = await asyncio.wait(
                [task_c2s, task_s2c],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel the remaining direction immediately to free up sockets
            for task in pending:
                task.cancel()
    except Exception as e:
        print(f"    [PROXY ERROR] {e}")

async def run_test():
    print("==================================================")
    print("STEP 1: Compiling Go Binaries...")
    print("==================================================")
    
    subprocess.run(["go", "build", "-o", "server.exe", "./cmd/server"], check=True)
    subprocess.run(["go", "build", "-o", "client.exe", "./cmd/client"], check=True)
    
    print("Compiled binaries successfully.")

    print("\n==================================================")
    print("STEP 2: Starting Go Relay Server...")
    print("==================================================")
    server_proc = subprocess.Popen([os.path.join(".", "server.exe")])
    await asyncio.sleep(1.5)  # Non-blocking wait for server to bind port 8080

    print("\n==================================================")
    print("STEP 3: Starting Chaos Proxy...")
    print("==================================================")
    
    proxy_server = await websockets.serve(proxy_handler, "127.0.0.1", PROXY_PORT)
    print(f"Chaos proxy listening on ws://127.0.0.1:{PROXY_PORT}/ws")

    print("\n==================================================")
    print("STEP 4: Spawning Client Replicas...")
    print("==================================================")
    
    doc_id = "chaos-doc"
    num_clients = 3
    client_processes = []
    
    for i in range(num_clients):
        site_id = f"Site_{chr(65+i)}"  # Site_A, Site_B, Site_C
        print(f"Spawning client: {site_id}")
        p = subprocess.Popen(
            [
                os.path.join(".", "client.exe"), 
                "-url", PROXY_URL, 
                "-doc", doc_id, 
                "-site", site_id, 
                "-duration", "4"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        client_processes.append((site_id, p))

    print("\n==================================================")
    print("STEP 5: Editing in Progress (Network Chaos Active)...")
    print("==================================================")

    results = {}
    loop = asyncio.get_running_loop()
    
    # Read outputs in executor threads so that event loop is not blocked
    for site_id, p in client_processes:
        stdout, stderr = await loop.run_in_executor(None, p.communicate)
        
        for line in stdout.splitlines():
            if line.startswith("RESULT:"):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    results[parts[1]] = parts[2]
        
        if p.returncode != 0:
            print(f"Client {site_id} failed with exit code {p.returncode}")
            print(f"Errors:\n{stderr}")

    print("\n==================================================")
    print("STEP 6: Stopping Server and Cleaning up Proxy...")
    print("==================================================")
    
    proxy_server.close()
    await proxy_server.wait_closed()
    
    for task in active_tasks:
        task.cancel()
        
    server_proc.terminate()
    server_proc.wait()
    
    try:
        os.remove("server.exe")
        os.remove("client.exe")
    except OSError:
        pass

    print("\n==================================================")
    print("STEP 7: Convergence Verification")
    print("==================================================")
    
    hashes = {}
    
    print("\nFinal Document States:")
    for site_id, text in results.items():
        h = hashlib.sha256(text.encode("utf-8")).hexdigest()
        hashes[site_id] = h
        print(f"  [{site_id}] Length: {len(text)} | Hash: {h[:10]}... | Text: {text!r}")

    unique_hashes = set(hashes.values())
    
    if len(results) < num_clients:
        print("\nERROR: Not all clients returned results successfully.")
        sys.exit(1)
        
    if len(unique_hashes) == 1:
        print("\nSUCCESS: All replica hashes match! CRDT RGA converged perfectly under chaos.")
        sys.exit(0)
    else:
        print("\nCRITICAL: Hashes DO NOT match. Document states diverged!")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_test())
