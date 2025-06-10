COPY dummy_table FROM 'dummy_export/dummy_table.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
