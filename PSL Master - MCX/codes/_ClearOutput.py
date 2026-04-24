import os
import csv
import glob
import shutil

BASE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.join(BASE, '..', 'backend_files')
OUTPUT, MODIFIED, SL_TIMES = (os.path.join(BACKEND, d) for d in ('codes_output', 'modified', 'sl_times'))


def matches(directory, code):
    """Files in `directory` whose space-separated name tokens contain `code`."""
    if not os.path.isdir(directory):
        return []
    return [f for f in os.listdir(directory)
            if code in os.path.splitext(f)[0].split(' ')]


def clear_code(code):
    """Delete per-code folder + matching files across modified/sl_times."""
    removed = 0
    out_dir = os.path.join(OUTPUT, f'{code}_output')
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir); removed += 1
    for d in (MODIFIED, SL_TIMES):
        for f in matches(d, code):
            try:
                os.remove(os.path.join(d, f)); removed += 1
            except OSError as e:
                print(f'  ! {f}: {e}')
    return removed


def wipe_all():
    """Delete everything remaining in the three backend dirs."""
    removed = 0
    for d in (OUTPUT, MODIFIED, SL_TIMES):
        for entry in (os.listdir(d) if os.path.isdir(d) else []):
            full = os.path.join(d, entry)
            try:
                (shutil.rmtree if os.path.isdir(full) else os.remove)(full); removed += 1
            except OSError as e:
                print(f'  ! {entry}: {e}')
    return removed


def stop(msg):
    print(msg)
    input("Press Enter to exit")
    raise SystemExit


# Discover one strategy per parameters/*.csv with its unique code list
strategies = []
for path in sorted(glob.glob(os.path.join(BASE, '..', 'parameters', '*.csv'))):
    name = os.path.splitext(os.path.basename(path))[0].removeprefix('Parameter_')
    with open(path, newline='', encoding='utf-8-sig') as f:
        codes = sorted({(r.get('code') or '').strip() for r in csv.DictReader(f)} - {''})
    strategies.append((name, codes))

if not strategies:
    stop("No parameter files found in ../parameters/.")

width = len(str(len(strategies)))
print("Strategies:")
print(f"  [{0:>{width}}] All")
for i, (name, _) in enumerate(strategies, 1):
    print(f"  [{i:>{width}}] {name}")

print("\nEnter numbers to clear (e.g. 1,3 or 1 3), 0 for All, Enter to cancel.")
choice = input("> ").strip()
if not choice:
    stop("Cancelled.")

try:
    nums = [int(x) for x in choice.replace(',', ' ').split()]
except ValueError:
    stop("Invalid input.")

wipe = 0 in nums
selected = strategies if wipe else [strategies[n - 1] for n in nums if 1 <= n <= len(strategies)]
if not selected:
    stop("Nothing selected.")

total = 0
for name, codes in selected:
    r = sum(clear_code(c) for c in codes)
    print(f"[{name}] cleared {r} item(s) across {len(codes)} code(s)")
    total += r

if wipe:
    r = wipe_all()
    if r:
        print(f"[leftovers] cleared {r} item(s)")
    total += r

print(f"\nDone. Total removed: {total}")
input("Press Enter to exit")
