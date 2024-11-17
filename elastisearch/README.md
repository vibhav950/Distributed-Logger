# Instructions to Set Up Elasticsearch


## Steps

1. **Download Elasticsearch**
    - Visit the [Elasticsearch download page](https://www.elastic.co/downloads/elasticsearch).

2. **Extract the Archive**
    - Extract the downloaded archive to a suitable location on your system. (windows)

4. **Start Elasticsearch**
    - Open a terminal or command prompt.
    - Navigate to the `bin` directory inside the extracted folder.
    - Run the following command to start Elasticsearch:
      ```sh
      ./elasticsearch
      ```
    - For Windows, use:
      ```sh
      elasticsearch.bat
      ```
    
5. **Reset Password**
    - Open another terminal

    - run
     ```sh
    ./elastisearch-reset-pasword.bat
    ```
    - Note down your password

5. **Verify Installation**
    - Open a web browser and go to `https://localhost:9200`.
    - You should see a JSON response with cluster information.


