import os
import argparse
import time
import subprocess
import multiprocessing

def run_cmds(cmds):
    for cmd in cmds:
        subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    start_time = time.time()
    args = argparse.ArgumentParser(description="Flag for test.")
    args.add_argument("--save_txt", type=str, help="file.")
    args.add_argument("--folder1", type=str, help="folder1.")
    args.add_argument("--folder2", type=str, help="folder2.")

    args = args.parse_args()
    cmds = []
    with open(args.save_txt, "r") as f:
        lines = f.readlines()
        i = 0
        for line in lines:
            cmd = "lcov --rc lcov_branch_coverage=1 -c -d {} -o {}/{}.info".format(line.split("\n")[0], args.folder1, i)
            cmds.append(cmd)
            i = i+1
    group_size = 1
    groups = [cmds[i:i+group_size] for i in range(0, len(cmds), group_size)]
    pool = multiprocessing.Pool(processes=len(cmds))
    for group in groups:
        pool.apply_async(run_cmds, args=(group,))
    pool.close()
    pool.join()
    time2 = time.time()
    print("[TIMER] lcov capture: ", time2 - start_time)
    cmd1= "lcov "
    for name in os.listdir(args.folder1):
        cmd1 += "-a " + os.path.join(args.folder1, name) + " "
    cmd1 += "-o {}/total.info".format(os.path.join(args.folder1))
    os.system(cmd1)
    time3 = time.time()
    print("[TIMER] lcov merge: ", time3 - time2)

    cmd2 = "lcov --extract {}/total.info \"*src/datasystem/*\" -o {}/total.info".format(os.path.join(args.folder1), args.folder2)
    os.system(cmd2)

    cmd3 = "lcov --remove {}/total.info \"*client/object_cache/device*\"  \"*worker/object_cache/device*\" \"*master/object_cache/device*\" \"*common/device/ascend*\" \"*/protos/*\" \"*/build/*\" -o {}/result.info".format(os.path.join(args.folder2), args.folder2)
    os.system(cmd3)
    print("[TIMER] lcov total time: ", time.time() - start_time)