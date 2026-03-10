upload_log_on_error() {
    echo "======== Start uploading logs ========"
    cd ${attach_workspace}
    tar -cvf datasystem_$(uname -m)_${BUILD_NUMBER}.tar.gz ${test_workspace}
    scp -o StrictHostKeyChecking=no -P ${af_node_port} -i ${af_node_key} datasystem_$(uname -m)_${BUILD_NUMBER}.tar.gz ${af_node_user}@${af_node_ip}:/data/logs/packages/yuanrong-datasystem/$(uname -m)/
    echo "Upload successful, log address is：https://build-logs.openeuler.openatom.cn:38080/packages/yuanrong-datasystem/$(uname -m)/datasystem_$(uname -m)_${BUILD_NUMBER}.tar.gz"
}

trap upload_log_on_error EXIT

test_workspace=${attach_workspace}/artifact
datasystem_dir=${WORKSPACE}/yuanrong-datasystem
attach_workspace=${WORKSPACE}/attach
output_cover=${attach_workspace}/cover
mkdir -p ${test_workspace}
mkdir -p ${attach_workspace}
mkdir -p ${output_cover}
chmod -R 755 ${datasystem_dir}
chmod -R 755 ${attach_workspace}
chmod -R 755 ${output_cover}

function run_compile() {
    cd ${datasystem_dir}
    set -xe

    export CCACHE_MAXSIZE=500G
    export CCACHE_IGNOREOPTIONS="-DGIT_HASH=* -DGIT_BRANCH=*"
    export LD_LIBRARY_PATH=${datasystem_dir}/output/service/lib:${LD_LIBRARY_PATH}
    echo $LD_LIBRARY_PATH
    export SOURCE_DATE_EPOCH=$(date +%s)
    export CTEST_OUTPUT_ON_FAILURE=True

    cd ${datasystem_dir}

    git config --global --add safe.directory ${datasystem_dir}
    git config core.filemode false
    git diff --name-only --diff-filter=AM HEAD~1 HEAD > ${attach_workspace}/diff_file_name
    git diff HEAD^ > ${attach_workspace}/diff.txt

    label=""
    tile=`curl -k --location --request GET "https://api.gitcode.com/api/v5/repos/openeuler/yuanrong-datasystem/pulls/${prid}/labels?access_token=hcVDUMFJcHjDdjRTWkKjbAAo" --header 'Content-Type: text/plain'`
    while read -r line
    do
        label1=`echo $line | awk '{print $NF}'`
        echo "$tile" | grep "$line\"" && label+=" $label1" || echo "no the label: $line"
    done < "${datasystem_dir}/.gitee/label.txt"
    label=$(echo "$label" | xargs)

    changes=$(curl -k --location --request GET "https://api.gitcode.com/api/v5/repos/openeuler/yuanrong-datasystem/pulls/${prid}/files" --header 'Content-Type: text/plain' | awk -F'"filename":"' '{for(i=1;i<=NF;i++){if($i~/^[^"]+"/){split($i,a,"\"");print a[1]}}}')
    OBJECT_MOD=false
    STREAM_MOD=false
    OTHER_MOD=false
    PY_SWITCH="off"
    EXAMPLE_SWITCH="off"

    while read -r line
    do
    change=$(echo $line)
    if [[ "$change" == *"object_cache"* || "$change" == *"kv_cache"* || "$change" == *"hetero_cache"* ]]; then
        OBJECT_MOD=true
    elif [[ "$change" == *"stream_cache"* ]]; then
        STREAM_MOD=true
    else
        OTHER_MOD=true
    fi

    BUILD_PYTHON=true
    case "$change" in
        *.py)
        PY_SWITCH="on"
        ;;
        *"example/"*)
        EXAMPLE_SWITCH="on"
        ;;
    esac
    if [[ "$BUILD_PYTHON" == "true" ]]; then
        PY_SWITCH="on"
    fi

    done <<< "$changes"

    mod_label=""
    if [[ "$OTHER_MOD" == "true" || ( "$OBJECT_MOD" == "true" && "$STREAM_MOD" == "true" ) ]]; then
    mod_label=""
    elif [[ "$OBJECT_MOD" == "true" ]]; then
    mod_label="object"
    elif [[ "$STREAM_MOD" == "true" ]]; then
    mod_label="stream"
    fi

    echo "label is $label, mod_label is $mod_label"
    echo "label: $label" > ${attach_workspace}/label.txt
    echo "mod_label: $mod_label" >> ${attach_workspace}/label.txt
    echo "python: $PY_SWITCH" >>${attach_workspace}/label.txt
    echo "example: $EXAMPLE_SWITCH" >> ${attach_workspace}/label.txt

    # Compile
    num_label=`echo "$label" | wc -w`
    tmp_cpus=16

    if [[ $num_label == 1 && "$label" != *"coverage"* ]]; then
    case $label in
    yr-ut-build-doc)
        exit 0
        ;;
    yr-ut-build-only)
        bash build.sh -t build -c on -j ${tmp_cpus} -X off && exit 0
        ;;
    esac

    else
    bash build.sh -t build -P ${PY_SWITCH} -c on -j ${tmp_cpus} -X off
    fi

    cd ${datasystem_dir}

    base_time=$(date +%s)
    process_dir() {
    local dir="$1"
    shift
    local files=("$@")
    cd "$dir"
    for file in "${files[@]}"; do
        if [[ -f "$file" ]]; then
        strip -s "$file" &
        else
        echo "Warning: File $file does not exist."
        fi
    done
    wait
    }
    process_dir "${datasystem_dir}/build/tests/st" \
    "ds_st_object_cache" "ds_st_stream_cache" "ds_st_kv_cache" "ds_st" "ds_device_llt" &
    process_dir "${datasystem_dir}/build/tests/ut" \
    "ds_ut" "ds_ut_stream" "ds_ut_object" &
    process_dir "${datasystem_dir}/build/src/datasystem/worker" "datasystem_worker" &
    process_dir "${datasystem_dir}/build/tests/perf/zmq" \
    "zmq_perf_agent" "zmq_perf_client" "zmq_perf_server" &
    wait
    echo "[TIMER] Strip cost: $(($(date +%s) - base_time)) seconds"
    rm -rf docs cmake k8s
    find . -type f \( -name "*.o" -o -name "*.a" \) -delete

    if [[ "$PY_SWITCH" == "off" && "$EXAMPLE_SWITCH" == "off" ]]; then
    rm -rf output/ example/ python/
    find . -type f \( -name "*.so" \) -delete
    fi
}

function get_label() {
  label=""
  mod_label=""

  label_txt="${attach_workspace}/label.txt"
  mapfile -t lines < "$label_txt"

  label="${lines[0]:7}"
  mod_label="${lines[1]:11}"

  echo "label: $label"
  echo "mod_label: $mod_label"
}

function collect_fail_logs() {
  echo -e "---- collecting fail testcases log files..."
  declare -a fail_dirs
  declare -a fail_ut
  declare -a fail_st
  while read -r line; do
    fail_dirs+=("${line#*:}")
  done <"${build_dir}/Testing/Temporary/LastTestsFailed.log"
  echo "Array numbers：${#fail_dirs[@]}"
  echo "${fail_dirs[@]}"
  for case_name in ${fail_dirs[@]}; do
    [[ -d ${build_dir}/tests/ut/ds/$case_name ]] && fail_ut+=" $case_name"
    [[ -d ${build_dir}/tests/st/ds/$case_name ]] && fail_st+=" $case_name"

    level1_case=$(echo "$case_name" | sed 's/\./.LEVEL1_/')
    [[ -d ${build_dir}/tests/ut/ds/$level1_case ]] && fail_ut+=" $level1_case"
    [[ -d ${build_dir}/tests/st/ds/$level1_case ]] && fail_st+=" $level1_case"

    if [[ $case_name == *".EXCLUSIVE_"* ]]; then
      level1_exclusive_case=$(echo "$case_name" | sed 's/\.EXCLUSIVE_/.LEVEL1_EXCLUSIVE_/')
      [[ -d ${build_dir}/tests/ut/ds/$level1_exclusive_case ]] && fail_ut+=" $level1_exclusive_case"
      [[ -d ${build_dir}/tests/st/ds/$level1_exclusive_case ]] && fail_st+=" $level1_exclusive_case"

      exclusive_level1_case=$(echo "$case_name" | sed 's/\.EXCLUSIVE_/.EXCLUSIVE_LEVEL1_/')
      [[ -d ${build_dir}/tests/ut/ds/$exclusive_level1_case ]] && fail_ut+=" $exclusive_level1_case"
      [[ -d ${build_dir}/tests/st/ds/$exclusive_level1_case ]] && fail_st+=" $exclusive_level1_case"
    fi
  done
  echo "${fail_ut}"
  echo "${fail_st}"
  if [ ${#fail_ut[@]} -gt 0 ]; then
    pushd "${build_dir}/tests/ut/ds"
    echo "failed_ut: ${fail_ut}"
    tar -zcf "${log_dir}/fail_${task_name}.tar.gz" ${fail_ut}
    popd
  elif [ ${#fail_st[@]} -gt 0 ]; then
    pushd "${build_dir}/tests/st/ds"
    echo "failed_st: ${fail_st}"
    tar -zcf "${log_dir}/fail_${task_name}.tar.gz" ${fail_st}
    popd
  elif [ ${#fail_dirs[@]} -gt 0 ]; then
    echo "Log not found" > ${log_dir}/fail_${task_name}.tar.gz
  else
    echo -e "---- all testcase run success, not log files need to collect!"
  fi

  echo -e "---- collect log files success, saved in ${log_dir}/fail_ut|st_${task_name}.tar.gz"
}

function handle_fail_tests() {
  if [ -d "${test_workspace}" ]; then
    collect_fail_logs
  fi
  echo "---- run datasystem testcases failed!"
  exit 1
}

function collect_coverage() {
  cd ${datasystem_dir}/build
  base_time=$(date +%s)
  modified_files=$(grep -v 'tests' diff_file_name | grep -E '\.(cpp|cc|cxx|h|hpp|hxx)$' | sed 's|.*/||; s/\.[^.]*$//')
  modified_headers=$(grep '\.h$' "${attach_workspace}/diff_file_name" | grep -v 'tests' | sed 's|.*/||')
  if [ -z "$modified_files" ]; then
    echo "Doesn't need collect coverage"
    return 0
  fi


  gcda_files=$(find ${datasystem_dir}/build -name "*.gcda")
  src_file_paths=()
  while IFS= read -r cpp_file; do
    src_file_paths+=("$cpp_file")
  done < <(find ${datasystem_dir}/src/ -type f -name "*.cpp")
  set +x
  gcda_to_check=()
  for gcda_file in $gcda_files; do
    base_name=$(basename "$gcda_file" .cpp.gcda)

    if [[ "$modified_files" == *"$base_name"* || "$modified_headers" == *"${base_name%_test}"* ]]; then
      gcda_to_check+=("$gcda_file")
      continue
    fi

    cpp_file_paths=()
    for cpp_file in "${src_file_paths[@]}"; do
      if [[ "$cpp_file" == *"${base_name}.cpp" ]]; then
        cpp_file_paths+=("$cpp_file")
      fi
    done

    for cpp_file_path in $cpp_file_paths; do
      for header in $modified_headers; do
        if head -n 100 "$cpp_file_path" |grep -q "#include.*$header"; then
          gcda_to_check+=("$gcda_file")
          break 2
        fi
        h_file_path="${cpp_file_path%.cpp}.h"
        if [ -f "$h_file_path" ]; then
          if head -n 100 "$h_file_path" |grep -q "#include.*$header"; then
            gcda_to_check+=("$gcda_file")
            break 2
          fi
        fi
      done
    done
  done
  set -x
  cover_path=${WORKSPACE}/cover
  mkdir -p ${cover_path}
  printf "%s\n" "${gcda_to_check[@]}" > ${cover_path}/gcda_files_to_check
  echo "[TIMER] Collect effective gcda files: $(($(date +%s) - base_time)) seconds"

  mkdir -p ${cover_path}/info_test1/
  mkdir -p ${cover_path}/info_test2/

  ls -lrt ${cover_path}
  ls -lrt ${cover_path}/info_test1
  ls -lrt ${cover_path}/info_test2
  cat ${cover_path}/gcda_files_to_check
  python3 ${datasystem_dir}/.gitee/findfile.py --save_txt="${cover_path}/gcda_files_to_check" --folder1="${cover_path}/info_test1" --folder2="${cover_path}/info_test2"

  cp ${cover_path}/info_test2/result.info ${output_cover}/result_${task_name}.info
  echo "[TIMER] Collect coverage total time: $(($(date +%s) - base_time)) seconds"
}

function run_test() {
    #level0
    task_name=level0
    log_dir=${test_workspace}/${task_name}
    build_dir="${datasystem_dir}/build"
    mkdir -p ${log_dir}
    echo 1 >${log_dir}/null.tar.gz

    get_label
    echo "label: $label"

    if ! [[ -z $label || $(echo $label |grep -E "ST|UT|level0|coverage") ]];then
    exit 0
    fi

    cd ${datasystem_dir}
    bash build.sh -t run_cpp -l '.*level0' -u 16   || handle_fail_tests
    if [[ "$label" != *"yr-ut-coverage-skip"* ]]; then
    collect_coverage
    fi

    #level 1
    task_name=level1
    log_dir=${test_workspace}/${task_name}
    mkdir -p ${log_dir}
    echo 1 >${log_dir}/null.tar.gz

    get_label
    echo "label: $label"

    if ! [[ -z $label || $(echo $label | grep -E "ST|UT|level1|coverage") ]]; then
    exit 0
    fi

    cd ${datasystem_dir}
    bash build.sh -t run_cpp -l ".*level1" -u 16 || handle_fail_tests

    if [[ "$label" != *"yr-ut-coverage-skip"* ]]; then
    collect_coverage
    fi

    # example
    cd ${datasystem_dir}
    bash build.sh -t run_python -P on -u 16
}

function run_coverage() {
    # coverage
    output_dir=${test_workspace}/coverage
    mkdir -p ${output_dir}

    if grep -q "coverage" "${attach_workspace}/label.txt"; then
      echo "CI skip coverage"
      echo "coverage_skip" > ${output_dir}/coverage.tar.gz
    elif [[ -z "$label" || $(echo $label |grep -E "ST|UT|level0|level1") ]];then
      cd ${output_cover}
      files=$(ls *.info 2>/dev/null) || true
      if [ -z "$files" ]; then
        echo "coverage_null" > ${output_dir}/coverage.tar.gz
        exit 0
      fi
      cmd="lcov "
      for file in ${files}; do
          cmd="${cmd} -a ${file}"
      done
      cmd="${cmd} -o total.info"
      eval ${cmd}

      lcov --diff total.info ${attach_workspace}/diff.txt -o coverage.info --path ${datasystem_dir}/

      genhtml -t datasystem -o coverage coverage.info
      tar -zcf "${output_dir}/coverage.tar.gz" coverage

      whitelist=(
        "src/datasystem/client/object_cache/object_client_impl.cpp"
        "src/datasystem/client/object_cache/object_client_impl.h"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_base_api.cpp"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_base_api.h"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_local_api.cpp"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_local_api.h"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_remote_api.cpp"
        "src/datasystem/client/object_cache/client_worker_api/client_worker_remtoe_api.h"
        "src/datasystem/common/constants.h"
      )

      while IFS= read -r file_path; do
        if [[ ! "$file_path" =~ ^src/.*\.(h|cpp)$ ]]; then
          continue
        fi
        for white_path in "${whitelist[@]}"; do
            if [[ "$file_path" == $white_path ]]; then
                echo "Skipping $file_path (whitelisted)"
                continue 2
            fi
        done

        file_name=$(basename "$file_path")
        parent_dir=$(dirname "$file_path")
        parent_dir_name=$(basename "$parent_dir")

        gcov_file=$(find . -name "${file_name}.gcov.html" | grep ${parent_dir_name})
        if [ -z "$gcov_file" ] || [ $(echo "$gcov_file" | wc -l) -gt 1 ]; then
          echo "No gcov file or multile files found: $file_path gcov_file: $gcov_file"
          break
        fi
        coverage_rate=`python3 ${datasystem_dir}/.gitee/get_html_data.py --dir_path="${gcov_file}"`
        if [ `echo "${coverage_rate}< 85"|bc ` -eq 1 ]; then
            echo "Coverage(${coverage_rate}) is not up to standard, file is ${file_name}"
            echo "coverage_failed" > ${output_dir}/coverage_failed.tar.gz
        fi
      done < "${attach_workspace}/diff_file_name"
    else
        echo "label: $label"
        echo "coverage_label" > ${output_dir}/coverage.tar.gz
    fi

    if [ -f "${output_dir}/coverage_failed.tar.gz" ]; then
        echo "coverage_failed.tar.gz exists, exiting with code 1."
        exit 1
    fi
}

function main() {
    run_compile
    run_test
    run_coverage
}

main
