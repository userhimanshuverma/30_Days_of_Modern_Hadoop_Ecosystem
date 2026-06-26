#!/usr/bin/env python3
import os
import time
import subprocess
import tempfile
import sys

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.returncode, result.stdout.decode('utf-8'), result.stderr.decode('utf-8')

def main():
    num_files = 2000
    file_size_kb = 10  # 10 KB each
    hdfs_target_dir = "/tmp/small_files_test"
    
    print("=========================================================")
    # 1. Clean HDFS target directory
    print("[+] Preparing HDFS target directory...")
    run_cmd(f"hdfs dfs -rm -r -f {hdfs_target_dir}")
    run_cmd(f"hdfs dfs -mkdir -p {hdfs_target_dir}")

    # 2. Write locally
    print(f"[+] Creating {num_files} small files ({file_size_kb} KB each) locally...")
    temp_dir = tempfile.mkdtemp()
    
    start_local = time.time()
    for i in range(num_files):
        file_path = os.path.join(temp_dir, f"file_{i}.txt")
        with open(file_path, "wb") as f:
            f.write(os.urandom(file_size_kb * 1024))
    end_local = time.time()
    print(f"[+] Local files generated in {end_local - start_local:.2f} seconds.")

    # 3. Upload sequentially to HDFS (Simulate typical naive pipeline)
    print(f"[+] Uploading {num_files} files sequentially to HDFS to simulate metadata overhead...")
    start_hdfs = time.time()
    errors = 0
    for i in range(num_files):
        local_path = os.path.join(temp_dir, f"file_{i}.txt")
        rc, out, err = run_cmd(f"hdfs dfs -put {local_path} {hdfs_target_dir}/file_{i}.txt")
        if rc != 0:
            errors += 1
            if errors <= 5:
                print(f"[-] Error uploading file_{i}.txt: {err}")
    end_hdfs = time.time()
    
    upload_duration = end_hdfs - start_hdfs
    print(f"[+] Uploaded {num_files - errors} files in {upload_duration:.2f} seconds.")
    print(f"[+] Average upload latency: {(upload_duration/num_files)*1000:.2f} ms per file.")

    # 4. Cleanup local temp files
    print("[+] Cleaning up local temporary directory...")
    for i in range(num_files):
        try:
            os.remove(os.path.join(temp_dir, f"file_{i}.txt"))
        except FileNotFoundError:
            pass
    os.rmdir(temp_dir)

    # 5. Measure NameNode namespace expansion
    print("[+] Fetching HDFS filesystem check summary:")
    rc, fsck_out, _ = run_cmd(f"hdfs fsck {hdfs_target_dir} -blocks")
    print(fsck_out)

    # 6. Compare with one large file upload
    print("=========================================================")
    print("[+] Comparing with equivalent size large file (20 MB) upload:")
    large_temp_path = "/tmp/large_20mb_file.bin"
    with open(large_temp_path, "wb") as f:
        f.write(os.urandom(num_files * file_size_kb * 1024)) # 2000 * 10KB = 20MB
    
    start_large = time.time()
    run_cmd(f"hdfs dfs -put -f {large_temp_path} {hdfs_target_dir}/large_20mb_file.bin")
    end_large = time.time()
    
    large_duration = end_large - start_large
    print(f"[+] Uploaded one 20MB file in {large_duration:.2f} seconds.")
    
    throughput_small = (20.0 / upload_duration) if upload_duration > 0 else 0
    throughput_large = (20.0 / large_duration) if large_duration > 0 else 0
    print(f"[+] Small Files Throughput: {throughput_small:.4f} MB/s")
    print(f"[+] Large File Throughput: {throughput_large:.4f} MB/s")
    print(f"[+] Speedup ratio of single large file: {large_duration / upload_duration if upload_duration > 0 else 0:.2f}x (lower is faster, comparing total upload speed)")
    print(f"    Notice how metadata negotiation makes many small writes orders of magnitude slower than a single large write.")
    
    # Cleanup large HDFS files
    run_cmd(f"hdfs dfs -rm -r -f {hdfs_target_dir}")
    os.remove(large_temp_path)
    print("=========================================================")

if __name__ == "__main__":
    main()
