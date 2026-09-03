import subprocess
import time
import sys
from PIL import Image, ImageDraw, ImageFont

WIDTH = 940
HEIGHT = 580
BG_COLOR = (40, 42, 54)        # Dracula Dark (#282a36)
HEADER_COLOR = (33, 34, 44)    # Dracula Header (#21222c)
TEXT_COLOR = (248, 248, 242)    # Foreground (#f8f8f2)
GREEN = (80, 250, 123)         # #50fa7b
CYAN = (139, 233, 253)         # #8be9fd
YELLOW = (241, 250, 140)       # #f1fa8c
MAGENTA = (255, 121, 198)      # #ff79c6
GRAY = (98, 114, 164)          # #6272a4
RED = (255, 85, 85)            # #ff5555

def get_font():
    try:
        return ImageFont.truetype("consola.ttf", 15)
    except IOError:
        try:
            return ImageFont.truetype("arial.ttf", 15)
        except IOError:
            return ImageFont.load_default()

FONT = get_font()

def parse_line_color(line):
    if "[Step" in line or "[Step 1]" in line or "[Step 2]" in line or "[Step 3]" in line or "[Step 4]" in line or "[Step 5]" in line:
        return YELLOW
    if "+ Build successful" in line or "CONVERGENCE REPORT" in line or "[SUCCESS]" in line or "---" in line:
        return GREEN
    if "Server A" in line or "Server 8080" in line or "SHA-256" in line or "REAL-TIME" in line or "===" in line:
        return CYAN
    if "Client A" in line or "Client B" in line or "Server B" in line or "Server 8082" in line:
        return MAGENTA
    if "Topology" in line or "Architecture" in line or "---" in line:
        return GRAY
    return TEXT_COLOR

def strip_ansi(text):
    # Strip ANSI color codes for clean canvas drawing
    import re
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def create_frame(rendered_lines):
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Draw Terminal Header Bar
    draw.rectangle([(0, 0), (WIDTH, 36)], fill=HEADER_COLOR)
    draw.ellipse([(15, 12), (27, 24)], fill=(255, 95, 86))    # Red dot
    draw.ellipse([(35, 12), (47, 24)], fill=(255, 189, 46))   # Yellow dot
    draw.ellipse([(55, 12), (67, 24)], fill=(39, 201, 63))    # Green dot

    draw.text((WIDTH // 2 - 150, 9), "powershell - P2P Gossip CRDT Sync", font=FONT, fill=GRAY)

    y = 52
    # Show last 22 lines if screen exceeds height
    visible_lines = rendered_lines[-22:] if len(rendered_lines) > 22 else rendered_lines
    for text, color in visible_lines:
        draw.text((25, y), text, font=FONT, fill=color)
        y += 22

    return img

def main():
    print("Executing live demo.py and capturing REAL-TIME output stream...")
    
    python_exe = sys.executable
    proc = subprocess.Popen(
        [python_exe, "demo.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    rendered_lines = [("$ python demo.py", TEXT_COLOR)]
    frames = []
    durations = []

    frames.append(create_frame(rendered_lines))
    durations.append(600)

    for line in iter(proc.stdout.readline, ''):
        raw = line.rstrip('\r\n')
        clean = strip_ansi(raw)
        if not clean:
            continue

        color = parse_line_color(clean)
        rendered_lines.append((clean, color))

        # Add frame for each new live line printed by demo.py
        frames.append(create_frame(rendered_lines))
        
        if "[SUCCESS]" in clean:
            durations.append(5000) # Hold final success screen for 5 seconds
        elif "[Step" in clean:
            durations.append(1200)
        else:
            durations.append(400)

    proc.wait()

    if len(frames) > 1:
        frames[0].save(
            "demo.gif",
            save_all=True,
            append_images=frames[1:],
            duration=durations,
            loop=0
        )
        print(f"SUCCESS: Captured {len(frames)} REAL execution frames into demo.gif!")
    else:
        print("ERROR: Failed to capture execution frames.")

if __name__ == "__main__":
    main()
