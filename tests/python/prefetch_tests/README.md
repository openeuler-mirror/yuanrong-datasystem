# prefetch/get/publish/create_dev_buffer
## test step
### Step 1: start worker
```
cd PATH_TO_ROOT && bash start_worker.sh
```
### Step 2: run test
```
cd PATH_TO_ROOT/tests/python
python -m unittest test_multi_key_prefetch.TestDeviceOcClientMethods.test_device_put_and_get

```
