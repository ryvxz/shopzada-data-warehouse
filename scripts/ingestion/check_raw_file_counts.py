import glob

file_types = {
    "Excel": ['.xls', '.xlsx'],
    "CSV": ['.csv'],
    "JSON": ['.json'],
    "Parquet": ['.parquet'],
    "Pickle": ['.pkl', '.pickle'],
    "HTML": ['.html', '.htm']
}

base_folder = "../../data/raw/"

counts = {key: 0 for key in file_types}

files_found = {key: [] for key in file_types}

all_files = glob.glob(base_folder + "**/*.*", recursive=True)

for f in all_files:
    f_lower = f.lower()
    for type_name, extensions in file_types.items():
        if any(f_lower.endswith(ext) for ext in extensions):
            counts[type_name] += 1
            files_found[type_name].append(f)

print("Files found per type:")
for type_name in counts:
    print(f"{type_name}: {counts[type_name]} files")
    for f in files_found[type_name]:
        print(f"  - {f}")