# Spark_Practice_MM

This application is built in Python based on Spark 2.0+ version and it includes main data processing function, shell script wrapper for main function and unit tests script. The generated results are stored in the `data/processed/` directory. All the required module can be install through pip install command. 

**Required Module:** `tornado, pytest, logging, findspark`

**Application Structure:**

    README.md

    data/

        events.csv

        impressions.csv

        processed/

            count_of_events.csv

            count_of_users.csv

    lib/

        schema.py

        utils.py

    scripts/

        process_data.sh

        calculate_attribution.py

    tests/

        conftest.py

        test_calculate_attribution.py


**Example Command for Launching Application:**
```
bash scripts/process_data.sh --partition 100 --driver_memory 3G --executor_memory 3G --executor_cores 1 --num_executors 30 --events_input_dir data/events.csv --impressions_input_dir data/impressions.csv --output_dir data --app_env dev
```
Noticed that `app_env` is `dev` when you launch the application in local machine. Otherwise, in the cluster environment, `app_env` should be `qa` or `prod`


**Example Command for Launching Unit Test:**
```
py.test tests/test_calculate_attribution.py -vv
```
