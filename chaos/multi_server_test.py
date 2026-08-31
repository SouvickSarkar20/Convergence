import asyncio
import subprocess
import os
import sys
import hashlib

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def run_multi_server_test():
    print("==================================================")
    print("STEP 1: Compiling Go Binaries...")
    print("==================================================")
    subprocess.run(["go", "build", "-o", "server.exe", "./cmd/server"], check=True)
    subprocess.run(["go", "build", "-o", "client.exe", "./cmd/client"], check=True)
    print("Compiled binaries successfully.")

    # Kill any existing server processes just in case
    if sys.platform == 'win32':
        subprocess.run(["taskkill", "/F", "/IM", "server.exe"], capture_output=True)

    print("\n==================================================")
    print("STEP 2: Starting Peer-Connected Servers...")
    print("==================================================")
    
    # Server A on port 8080
    server_a = subprocess.Popen([os.path.join(".", "server.exe"), "-port", "8080"])
    
    # Server B on port 8082, peering with Server A
    server_b = subprocess.Popen([
        os.path.join(".", "server.exe"), 
        "-port", "8082", 
        "-peers", "ws://127.0.0.1:8080/peer"
    ])

    await asyncio.sleep(2.0)  # Wait for servers to start and establish peer connection

    print("\n==================================================")
    print("STEP 3: Spawning Clients on Separate Servers...")
    print("==================================================")
    
    # Client A connects to Server A
    p_client_a = subprocess.Popen([
        os.path.join(".", "client.exe"),
        "-url", "ws://127.0.0.1:8080/ws",
        "-doc", "gossip-doc",
        "-site", "Site_A",
        "-duration", "3",
        "-delete", "0.2"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Client A spawned targeting Server A (port 8080)")

    # Client B connects to Server B
    p_client_b = subprocess.Popen([
        os.path.join(".", "client.exe"),
        "-url", "ws://127.0.0.1:8082/ws",
        "-doc", "gossip-doc",
        "-site", "Site_B",
        "-duration", "3",
        "-delete", "0.2"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Client B spawned targeting Server B (port 8082)")

    print("\n==================================================")
    print("STEP 4: Editing in Progress (Cross-Server Syncing)...")
    print("==================================================")

    # Wait for clients to complete their edits and sync
    loop = asyncio.get_running_loop()
    stdout_a, stderr_a = await loop.run_in_executor(None, p_client_a.communicate)
    stdout_b, stderr_b = await loop.run_in_executor(None, p_client_b.communicate)

    print("\n==================================================")
    print("STEP 5: Stopping Servers and Verification...")
    print("==================================================")

    server_a.terminate()
    server_b.terminate()
    server_a.wait()
    server_b.wait()

    # Parse final document states
    results = {}
    for stdout in [stdout_a, stdout_b]:
        for line in stdout.splitlines():
            if line.startswith("RESULT:"):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    results[parts[1]] = parts[2]

    print("\nFinal Document States:")
    hashes = {}
    for site, text in results.items():
        h = hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]
        hashes[site] = h
        print(f"  [{site}] Length: {len(text)} | Hash: {h} | Text: '{text}'")

    # Assert matching hashes
    if len(hashes) < 2:
        print("\nFAILURE: One or more clients did not produce a result.")
        sys.exit(1)
        
    unique_hashes = set(hashes.values())
    if len(unique_hashes) == 1:
        print("\nSUCCESS: All replica hashes match across peer-connected servers!")
        print("CRDT RGA converged perfectly via Gossip Replication.")
        sys.exit(0)
    else:
        print("\nFAILURE: Document states diverged between the servers.")
        print("\n--- Client A stdout ---")
        print(stdout_a)
        print("\n--- Client A stderr ---")
        print(stderr_a)
        print("\n--- Client B stdout ---")
        print(stdout_b)
        print("\n--- Client B stderr ---")
        print(stderr_b)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_multi_server_test())
