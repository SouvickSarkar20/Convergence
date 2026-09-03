import subprocess
import time
import os
import sys
import hashlib
import threading

# ANSI Color Codes for Rich Terminal Styling
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
RED = "\033[91m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

def print_header(title):
    print("\n" + BOLD + CYAN + "=" * 60 + RESET)
    print(BOLD + CYAN + f"  {title}" + RESET)
    print(BOLD + CYAN + "=" * 60 + RESET + "\n")

def print_step(step_num, message):
    print(f"{BOLD}{YELLOW}[Step {step_num}]{RESET} {message}")
    time.sleep(1.0)

def main():
    print_header("REAL-TIME COLLABORATIVE CRDT DEMONSTRATION")
    print(f"{BOLD}Architecture:{RESET} Multi-Server P2P Gossip Relay (RGA CRDT Algorithm)")
    print(f"{BOLD}Topology:{RESET} Client A -> Server A (8080) <== Gossip ==> Server B (8082) <- Client B\n")
    time.sleep(1.5)

    # Step 1: Build Binaries
    print_step(1, "Compiling Go binaries (cmd/server, cmd/client)...")
    try:
        subprocess.run(["go", "build", "-o", "server.exe", "./cmd/server"], check=True)
        subprocess.run(["go", "build", "-o", "client.exe", "./cmd/client"], check=True)
        print(f"  {GREEN}+ Build successful: server.exe, client.exe{RESET}\n")
    except Exception as e:
        print(f"  {RED}x Build failed: {e}{RESET}")
        sys.exit(1)

    time.sleep(1)

    # Step 2: Start Servers
    print_step(2, "Spinning up P2P Mesh Cluster...")
    server_a = subprocess.Popen(
        ["./server.exe", "-port", "8080", "-peers", "ws://localhost:8082/peer"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    print(f"  {CYAN}* Server A listening on ws://localhost:8080/ws (Peer -> 8082){RESET}")

    time.sleep(0.5)

    server_b = subprocess.Popen(
        ["./server.exe", "-port", "8082", "-peers", "ws://localhost:8080/peer"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    print(f"  {CYAN}* Server B listening on ws://localhost:8082/ws (Peer -> 8080){RESET}\n")

    time.sleep(1.5)

    # Step 3: Spawn Clients
    print_step(3, "Spawning concurrent collaborative typists...")
    
    os.makedirs("diagnostics", exist_ok=True)
    out_a = open("diagnostics/demo_client_a.txt", "w")
    out_b = open("diagnostics/demo_client_b.txt", "w")

    client_a = subprocess.Popen(
        ["./client.exe", "-url", "ws://localhost:8080/ws", "-site", "Site_A", "-doc", "demo-doc", "-duration", "4", "-delete", "0.15"],
        stdout=out_a,
        stderr=subprocess.DEVNULL
    )
    print(f"  {MAGENTA}> Client A (Site_A) connected to Server A{RESET}")

    client_b = subprocess.Popen(
        ["./client.exe", "-url", "ws://localhost:8082/ws", "-site", "Site_B", "-doc", "demo-doc", "-duration", "4", "-delete", "0.15"],
        stdout=out_b,
        stderr=subprocess.DEVNULL
    )
    print(f"  {MAGENTA}> Client B (Site_B) connected to Server B{RESET}\n")

    # Step 4: Simulate Live Editing & Stream Chaos Ops
    print_step(4, "Executing concurrent insertions & deletions under Gossip Replication...")
    print(f"  {GRAY}--- [LIVE P2P CHAOS STREAM] ---{RESET}")
    
    stop_event = threading.Event()

    def log_reader(pipe, server_name, color):
        for line in iter(pipe.readline, ''):
            if stop_event.is_set():
                break
            if "[P2P]" in line:
                clean = line.strip().split("[P2P]", 1)[1]
                print(f"  {color}[{server_name}]{RESET} {clean}")
        pipe.close()

    t_a = threading.Thread(target=log_reader, args=(server_a.stderr, "Server 8080", CYAN), daemon=True)
    t_b = threading.Thread(target=log_reader, args=(server_b.stderr, "Server 8082", MAGENTA), daemon=True)
    t_a.start()
    t_b.start()

    client_a.wait()
    client_b.wait()
    out_a.close()
    out_b.close()
    time.sleep(1.0)
    stop_event.set()

    print(f"  {GRAY}--- [Gossip Replication Settled] ---{RESET}\n")

    # Step 5: Shut down cluster & analyze convergence
    print_step(5, "Stopping server nodes and verifying eventual consistency...")
    server_a.terminate()
    server_b.terminate()
    server_a.wait()
    server_b.wait()

    # Parse Outputs
    def parse_client_output(filepath):
        with open(filepath, "r") as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
        doc_text = ""
        for l in lines:
            if l.startswith("RESULT:"):
                parts = l.split(":", 2)
                if len(parts) >= 3:
                    doc_text = parts[2]
        return doc_text

    text_a = parse_client_output("diagnostics/demo_client_a.txt")
    text_b = parse_client_output("diagnostics/demo_client_b.txt")

    hash_a = hashlib.sha256(text_a.encode("utf-8")).hexdigest()[:10]
    hash_b = hashlib.sha256(text_b.encode("utf-8")).hexdigest()[:10]

    print(BOLD + GREEN + "-" * 60 + RESET)
    print(BOLD + GREEN + "                    CONVERGENCE REPORT" + RESET)
    print(BOLD + GREEN + "-" * 60 + RESET)
    print(f"  {BOLD}Client A (Site_A):{RESET} '{text_a}'")
    print(f"  {BOLD}SHA-256 Hash:{RESET}     {hash_a}")
    print()
    print(f"  {BOLD}Client B (Site_B):{RESET} '{text_b}'")
    print(f"  {BOLD}SHA-256 Hash:{RESET}     {hash_b}")
    print(BOLD + GREEN + "-" * 60 + RESET)

    if text_a == text_b and hash_a == hash_b:
        print(f"\n  {BOLD}{GREEN}[SUCCESS] Eventual consistency achieved! All replicas converged perfectly.{RESET}\n")
    else:
        print(f"\n  {BOLD}{RED}[FAILURE] Document states diverged!{RESET}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
