import os
import re
import csv
import sys
import subprocess
from collections import defaultdict
import concurrent.futures
import threading

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
FILTER_MAP = {
    'R': 28943,
    'L': 25576,
    'G': 28944,
    'B': 28945,
    'H': 4410,
    'O': 4415,
    'S': 4420
}

# Pre-compile Regex Patterns for Speed
# We store these globally so we don't rebuild them 5000 times.
PATTERNS = {
    'xisf': {
        'type': re.compile(r'<FITSKeyword\s+name="(?:IMAGETYP|TYPE)"\s+value="([^"]+)"', re.IGNORECASE),
        'date': re.compile(r'<FITSKeyword\s+name="(?:DATE-LOC|DATE-OBS|DATE)"\s+value="([^"]+)"', re.IGNORECASE),
        'filter': re.compile(r'<FITSKeyword\s+name="(?:FILTER|FILT)"\s+value="([^"]+)"', re.IGNORECASE),
        'exposure': re.compile(r'<FITSKeyword\s+name="(?:EXPTIME|EXPOSURE)"\s+value="([^"]+)"', re.IGNORECASE),
        'gain': re.compile(r'<FITSKeyword\s+name="(?:GAIN|ISO)"\s+value="([^"]+)"', re.IGNORECASE),
        'binning': re.compile(r'<FITSKeyword\s+name="(?:XBINNING|BINNING)"\s+value="([^"]+)"', re.IGNORECASE),
    },
    'fits': {
        'type': re.compile(r'(?:IMAGETYP|TYPE)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
        'date': re.compile(r'(?:DATE-LOC|DATE-OBS|DATE)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
        'filter': re.compile(r'(?:FILTER|FILT)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
        'exposure': re.compile(r'(?:EXPTIME|EXPOSURE)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
        'gain': re.compile(r'(?:GAIN|ISO)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
        'binning': re.compile(r'(?:XBINNING|BINNING)\s*=\s*(?:\'([^\']*)\'|([0-9\.-]+))', re.IGNORECASE),
    }
}

# ---------------------------------------------------------
# GUI / INPUT HELPERS
# ---------------------------------------------------------
def get_file_mac_native():
    try:
        cmd = """
        set theFile to choose file with prompt "Select PixInsight WBPP Log File" of type {"log", "txt"}
        return POSIX path of theFile
        """
        result = subprocess.run(['osascript', '-e', cmd], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None

def get_input_mac_native(prompt, default_val):
    try:
        cmd = f"""
        set theResponse to display dialog "{prompt}" default answer "{default_val}" with title "Input"
        return text returned of theResponse
        """
        result = subprocess.run(['osascript', '-e', cmd], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return default_val

def get_user_inputs():
    log_path = None
    bortle = 4

    if sys.platform == 'darwin':
        log_path = get_file_mac_native()
        if log_path:
            b_str = get_input_mac_native("Enter Bortle Scale (1-9):", "4")
            if b_str and b_str.isdigit():
                bortle = int(b_str)
            return log_path, bortle
        if log_path is None:
            pass 

    try:
        import tkinter as tk
        from tkinter import filedialog, simpledialog
        
        root = tk.Tk()
        root.withdraw()

        log_path = filedialog.askopenfilename(
            title="Select PixInsight WBPP Log File",
            filetypes=[("Log Files", "*.log"), ("All Files", "*.*")]
        )
        
        if log_path:
            b_val = simpledialog.askinteger("Bortle", "Enter Bortle scale (1-9):", initialvalue=4, minvalue=1, maxvalue=9)
            if b_val:
                bortle = b_val
            return log_path, bortle
            
    except Exception as e:
        print(f"GUI Warning: {e}")

    if not log_path:
        print("\n--- GUI Failed or Cancelled. Using Command Line ---")
        log_path = input("Please paste the path to your log file: ").strip().strip("'").strip('"')
        
    return log_path, bortle

def show_message(title, message, is_error=False):
    print(f"\n[{title}] {message}")
    if sys.platform == 'darwin':
        icon = "stop" if is_error else "note"
        cmd = f'display dialog "{message}" with title "{title}" buttons {{"OK"}} default button "OK" with icon {icon}'
        subprocess.run(['osascript', '-e', cmd], stderr=subprocess.DEVNULL)
        return
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        if is_error:
            messagebox.showerror(title, message)
        else:
            messagebox.showinfo(title, message)
    except:
        pass

# ---------------------------------------------------------
# CORE LOGIC
# ---------------------------------------------------------
def extract_val(match):
    """Helper to extract clean value from regex match object."""
    if not match:
        return None
    # FITS regex has two groups (quoted string OR number), XISF has one.
    # We find the first non-None group.
    val = next((g for g in match.groups() if g is not None), None)
    if val:
        return val.strip("'").strip('"')
    return None

def process_single_file(file_path):
    """
    Worker function to process exactly one file. 
    Returns a tuple: (key_data, success_boolean)
    """
    if not os.path.exists(file_path):
        return None

    header_chunk_size = 50000 
    meta = {}
    
    try:
        is_xisf = file_path.lower().endswith('.xisf')
        patterns = PATTERNS['xisf'] if is_xisf else PATTERNS['fits']

        with open(file_path, 'rb') as f:
            header_data = f.read(header_chunk_size)
            header_text = header_data.decode('latin-1', errors='ignore')

            # 1. Type (Fast check: if not LIGHT, abort immediately to save time)
            match = patterns['type'].search(header_text)
            val = extract_val(match)
            if not val or 'LIGHT' not in val.upper():
                return None

            # 2. Extract remaining fields only if it's a LIGHT frame
            # Filter
            match = patterns['filter'].search(header_text)
            raw_filter = extract_val(match) or 'Unknown'
            
            # Exposure
            match = patterns['exposure'].search(header_text)
            val = extract_val(match)
            exposure = f"{float(val):.2f}" if val else "0"

            # Date
            match = patterns['date'].search(header_text)
            val = extract_val(match)
            date = val.split('T')[0] if val else 'Unknown'

            # Gain
            match = patterns['gain'].search(header_text)
            val = extract_val(match)
            gain = str(int(float(val))) if val else "0"

            # Binning
            match = patterns['binning'].search(header_text)
            val = extract_val(match)
            binning = val if val else "1"

            # Map Filter
            filter_id = FILTER_MAP.get(raw_filter, FILTER_MAP.get(raw_filter[0], raw_filter))

            return (date, filter_id, exposure, binning, gain)

    except Exception:
        return None

def process_log(log_path, bortle_val):
    if not log_path or not os.path.exists(log_path):
        show_message("Error", "File not found.", True)
        return

    print(f"Reading log: {log_path}...")
    
    valid_paths = []
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Simple regex to find paths in WBPP logs
            matches = re.findall(r'\[true,\s*\"(.*?)\"', content)
            valid_paths = list(set(matches))
    except Exception as e:
        show_message("Error", f"Could not read log:\n{e}", True)
        return

    if not valid_paths:
        show_message("Warning", "No registered file paths found in this log.", True)
        return

    print(f"Found {len(valid_paths)} potential files. Scanning headers in parallel...")

    grouped_data = defaultdict(int)
    count_processed = 0
    
    # MULTI-THREADING MAGIC HERE
    # Max workers = 20 is usually a safe sweet spot for file I/O on Windows
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Submit all tasks
        futures = {executor.submit(process_single_file, path): path for path in valid_paths}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result_key = future.result()
            
            if result_key:
                grouped_data[result_key] += 1
                count_processed += 1
            
            # Progress indicator every 50 files
            if (i + 1) % 50 == 0:
                print(f"Scanned {i + 1}/{len(valid_paths)} files...", end='\r')

    print(f"\nDone! Processed {count_processed} valid light frames.")

    output_dir = os.path.dirname(log_path)
    output_csv = os.path.join(output_dir, "astrobin_import.csv")
    
    try:
        with open(output_csv, 'w', newline='') as csvfile:
            fieldnames = ['date', 'filter', 'number', 'duration', 'binning', 'gain', 'bortle']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for key in sorted(grouped_data.keys()):
                date, flt_id, dur, binning, gain = key
                writer.writerow({
                    'date': date,
                    'filter': flt_id,
                    'number': grouped_data[key],
                    'duration': dur,
                    'binning': binning,
                    'gain': gain,
                    'bortle': bortle_val
                })
        
        msg = f"Processed {count_processed} files.\nCSV saved to:\n{output_csv}"
        show_message("Success", msg)

    except Exception as e:
        show_message("Error", f"Failed to write CSV:\n{e}", True)

if __name__ == "__main__":
    # Windows/PyInstaller multiprocessing fix
    # (Though we are using threads, this is good practice to include)
    import multiprocessing
    multiprocessing.freeze_support()
    
    path, bortle = get_user_inputs()
    if path:
        process_log(path, bortle)