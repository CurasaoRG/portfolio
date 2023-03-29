MERGE INTO RASHITGAFAROVYANDEXRU__DWH.global_metrics AS gm USING
    (SELECT *,
            now()::date AS load_dt,
            'STG' AS load_src
    FROM
        (SELECT t.date_update,
                t.cnt_transactions,
                t.cnt_accounts_make_transactions,
                t.currency_from,
                t.avg_transactions_per_account,
                t.amount_total_local*c.currency_with_div AS amount_total
    FROM
        (SELECT count(operation_id) AS cnt_transactions,
                count(DISTINCT account_number_from) AS cnt_accounts_make_transactions,
                currency_code AS currency_from,
                sum(amount)/count(DISTINCT account_number_from) AS avg_transactions_per_account,
                sum(amount) AS amount_total_local,
                transaction_dt::date AS date_update
            FROM RASHITGAFAROVYANDEXRU__STAGING.transactions t
            WHERE status = 'done' AND account_number_from>0
            AND load_dt > '{inc}'::date
            GROUP BY currency_code,
                    date_update) t
    LEFT JOIN RASHITGAFAROVYANDEXRU__STAGING.currencies c ON t.currency_from = c.currency_code
        AND t.date_update = c.date_update::date
    WHERE c.currency_code_with = 420) ctt) ct ON (gm.date_update = ct.date_update
                                                    AND gm.currency_from = ct.currency_from) 
    WHEN NOT MATCHED THEN
        INSERT (date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions,
                load_dt,
                load_src)
        VALUES (ct.date_update, 
                ct.currency_from, 
                ct.amount_total, 
                ct.cnt_transactions, 
                ct.avg_transactions_per_account, 
                ct.cnt_accounts_make_transactions, 
                ct.load_dt, 
                ct.load_src);