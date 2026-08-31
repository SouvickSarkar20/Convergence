import asyncio
# pyrefly: ignore [missing-import]
import websockets
import random
import subprocess
import hashlib
import sys
import os
import time
# pyrefly: ignore [missing-import]
import matplotlib.pyplot as plt

# Workaround for Windows asyncio ProactorEventLoop bugs during socket serving
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROXY_PORT = 8081
SERVER_PORT = 8080
UPSTREAM_URL = f"ws://127.0.0.1:{SERVER_PORT}/ws"
PROXY_URL = f"ws://127.0.0.1:{PROXY_PORT}/ws"
DIRECT_URL = f"ws://127.0.0.1:{SERVER_PORT}/ws"

active_tasks = []

async def send_delayed(target_ws, message, delay):
    await asyncio.sleep(delay)
    try:
        await target_ws.send(message)
    except Exception:
        pass

async def forward_with_chaos(source_ws, target_ws, delay_max):
    try:
        async for message in source_ws:
            # 30% chance of introducing delay (between 0 and delay_max)
            if delay_max > 0 and random.random() < 0.30:
                delay = random.uniform(0.01, delay_max)
                task = asyncio.create_task(send_delayed(target_ws, message, delay))
                active_tasks.append(task)
            else:
                try:
                    await target_ws.send(message)
                except Exception:
                    pass
    except websockets.exceptions.ConnectionClosed:
        pass

async def proxy_handler(client_ws, delay_max):
    try:
        async with websockets.connect(UPSTREAM_URL) as server_ws:
            task_c2s = asyncio.create_task(forward_with_chaos(client_ws, server_ws, delay_max))
            task_s2c = asyncio.create_task(forward_with_chaos(server_ws, client_ws, delay_max))
            done, pending = await asyncio.wait(
                [task_c2s, task_s2c],
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
    except Exception as e:
        pass

async def run_single_simulation(duration, delete_ratio, delay_max=0):
    """
    Runs a single simulation run.
    If delay_max > 0, routes clients through the Chaos Proxy.
    If delay_max == 0, connects clients directly to the Go server.
    """
    # Start Go Server
    server_proc = subprocess.Popen([os.path.join(".", "server.exe")])
    await asyncio.sleep(1.2)  # Wait for bind

    proxy_server = None
    target_url = DIRECT_URL

    if delay_max > 0:
        target_url = PROXY_URL
        # Start Proxy with specified delay
        handler = lambda ws: proxy_handler(ws, delay_max)
        proxy_server = await websockets.serve(handler, "127.0.0.1", PROXY_PORT)

    # Spawn 3 clients
    num_clients = 3
    client_processes = []
    
    for i in range(num_clients):
        site_id = f"Site_{chr(65+i)}"
        p = subprocess.Popen(
            [
                os.path.join(".", "client.exe"), 
                "-url", target_url, 
                "-doc", "perf-doc", 
                "-site", site_id, 
                "-duration", str(duration),
                "-delete", str(delete_ratio)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        client_processes.append((site_id, p))

    # Read outputs in executor thread
    results = {}
    metrics = {}
    loop = asyncio.get_running_loop()
    
    for site_id, p in client_processes:
        stdout, stderr = await loop.run_in_executor(None, p.communicate)
        
        for line in stdout.splitlines():
            if line.startswith("RESULT:"):
                parts = line.split(":", 2)
                if len(parts) >= 3:
                    results[parts[1]] = parts[2]
            elif line.startswith("METRICS:"):
                parts = line.split(":")
                if len(parts) >= 6:
                    # site: total_nodes: visible_nodes: last_local_edit: last_update
                    metrics[parts[1]] = {
                        "total_nodes": int(parts[2]),
                        "visible_nodes": int(parts[3]),
                        "last_local_edit": int(parts[4]),
                        "last_update": int(parts[5]),
                    }

    # Teardown
    if proxy_server:
        proxy_server.close()
        await proxy_server.wait_closed()
        
    for task in active_tasks:
        task.cancel()
    active_tasks.clear()

    server_proc.terminate()
    server_proc.wait()
    await asyncio.sleep(0.5)

    return results, metrics

async def run_experiments():
    print("==================================================")
    print("STEP 1: Compiling Go Binaries...")
    print("==================================================")
    subprocess.run(["go", "build", "-o", "server.exe", "./cmd/server"], check=True)
    subprocess.run(["go", "build", "-o", "client.exe", "./cmd/client"], check=True)
    print("Compiled binaries successfully.")

    # Create output directory for plots
    os.makedirs(os.path.join("chaos", "plots"), exist_ok=True)

    # --------------------------------------------------
    # EXPERIMENT 1: Network Latency Jitter vs Convergence Time
    # --------------------------------------------------
    print("\n==================================================")
    print("EXPERIMENT 1: Network Latency vs. Convergence Time")
    print("==================================================")
    
    delays_ms = [0, 50, 100, 200, 400]
    convergence_times = []

    for d in delays_ms:
        print(f"Running simulation with max jitter delay: {d}ms...")
        _, metrics = await run_single_simulation(duration=3, delete_ratio=0.2, delay_max=(d / 1000.0))
        
        # Calculate convergence time
        if len(metrics) == 3:
            max_local_edit = max(m["last_local_edit"] for m in metrics.values())
            max_update = max(m["last_update"] for m in metrics.values())
            # Convergence time in milliseconds
            conv_time = max(0.0, (max_update - max_local_edit) / 1e6)
            convergence_times.append(conv_time)
            print(f"  -> System settled in {conv_time:.2f} ms")
        else:
            print(f"  -> Error: metrics not collected for all clients. Using fallback.")
            convergence_times.append(0.0)

    # Plot Experiment 1
    plt.figure(figsize=(8, 5))
    plt.plot(delays_ms, convergence_times, marker='o', linewidth=2, color='#1f77b4')
    plt.title('CRDT Convergence Time vs. Max Network Jitter', fontsize=12, fontweight='bold', pad=15)
    plt.xlabel('Max Network Jitter Delay (ms)', fontsize=10)
    plt.ylabel('Convergence Settling Time (ms)', fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join("chaos", "plots", "convergence_time.png"), dpi=150)
    plt.close()
    print("Saved plot: chaos/plots/convergence_time.png")

    # --------------------------------------------------
    # EXPERIMENT 2: Deletion Ratio vs. Tombstone Footprint
    # --------------------------------------------------
    print("\n==================================================")
    print("EXPERIMENT 2: Deletion Ratio vs. Tombstone Footprint")
    print("==================================================")
    
    delete_ratios = [0.1, 0.3, 0.5]
    total_nodes_list = []
    visible_chars_list = []
    tombstones_list = []

    for r in delete_ratios:
        print(f"Running simulation with deletion probability: {int(r*100)}%...")
        _, metrics = await run_single_simulation(duration=5, delete_ratio=r, delay_max=0)
        
        if len(metrics) == 3:
            avg_total = sum(m["total_nodes"] for m in metrics.values()) / 3.0
            avg_visible = sum(m["visible_nodes"] for m in metrics.values()) / 3.0
            avg_tombstones = avg_total - avg_visible
            
            total_nodes_list.append(avg_total)
            visible_chars_list.append(avg_visible)
            tombstones_list.append(avg_tombstones)
            print(f"  -> Average Total Nodes: {avg_total:.1f} | Visible: {avg_visible:.1f} | Tombstones: {avg_tombstones:.1f}")
        else:
            total_nodes_list.append(0)
            visible_chars_list.append(0)
            tombstones_list.append(0)

    # Plot Experiment 2
    x_labels = [f"{int(r*100)}% Deletes" for r in delete_ratios]
    plt.figure(figsize=(8, 5))
    
    bar_width = 0.35
    index = range(len(delete_ratios))
    
    plt.bar(index, visible_chars_list, bar_width, label='Visible Characters (Active Text)', color='#2ca02c')
    plt.bar(index, tombstones_list, bar_width, bottom=visible_chars_list, label='Tombstones (Deleted Metadata)', color='#d62728')
    
    plt.title('CRDT Node Memory Footprint vs. Deletion Ratio', fontsize=12, fontweight='bold', pad=15)
    plt.xlabel('Configured Deletion Probability', fontsize=10)
    plt.ylabel('Node Count in Memory', fontsize=10)
    plt.xticks(index, x_labels)
    plt.legend(loc='upper left')
    plt.grid(True, axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join("chaos", "plots", "tombstone_footprint.png"), dpi=150)
    plt.close()
    print("Saved plot: chaos/plots/tombstone_footprint.png")

    # Clean up binaries
    try:
        os.remove("server.exe")
        os.remove("client.exe")
    except OSError:
        pass

    print("\n==================================================")
    print("METRICS HARVEST COMPLETED SUCCESSFULLY!")
    print("==================================================")

if __name__ == "__main__":
    # Ensure any previous server process is cleaned up before starting
    if sys.platform == 'win32':
        subprocess.run(["taskkill", "/F", "/IM", "server.exe"], capture_output=True)
        
    asyncio.run(run_experiments())
