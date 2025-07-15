CREATE TABLE abtest_query_cache (
    id INT AUTO_INCREMENT PRIMARY KEY,
    query_type VARCHAR(64) NOT NULL,
    experiment_name VARCHAR(128) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    result_json MEDIUMTEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
