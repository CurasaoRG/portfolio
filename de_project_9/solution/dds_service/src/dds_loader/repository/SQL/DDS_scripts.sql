---h_user---
INSERT INTO  dds.h_user
                    (h_user_pk,
                    user_id,
                    load_dt,
                    load_src)
                    values
                    ( %(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_user_pk)
                    DO NOTHING;
---h_product---
INSERT INTO  dds.h_product
                    (h_product_pk,
                    product_id,
                    load_dt,
                    load_src)
                    values
                    ( %(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_product_pk)
                    DO NOTHING;
---h_category---
INSERT INTO  dds.h_category
                    (h_category_pk,
                    category_name,
                    load_dt,
                    load_src)
                    values
                    ( %(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_category_pk)
                    DO NOTHING;
---h_restaurant---
INSERT INTO  dds.h_restaurant
                    (h_restaurant_pk,
                    restaurant_id,
                    load_dt,
                    load_src)
                    values
                    ( %(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_restaurant_pk)
                    DO NOTHING;
---h_order---
INSERT INTO  dds.h_order
                    (h_order_pk,
                    order_id, 
                    order_dt,
                    load_dt,
                    load_src)
                    values
                    ( %(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_order_pk)
                    DO NOTHING;
---l_order_product---
INSERT INTO  dds.l_order_product(
hk_order_product_pk,
h_order_pk,
h_product_pk,
load_dt,
load_src)
values(
     %(hk_order_product_pk)s,
    %(h_order_pk)s,
    %(h_product_pk)s,
    %(load_dt)s,
    %(load_src)s)
ON CONFLICT (hk_order_product_pk) DO NOTHING;
---l_product_restaurant---
INSERT INTO  dds.l_product_restaurant(
hk_product_restaurant_pk,
h_product_pk,
h_restaurant_pk,
load_dt,
load_src)
values(
     %(hk_product_restaurant_pk)s,
    %(h_product_pk)s,
    %(h_restaurant_pk)s,
    %(load_dt)s,
    %(load_src)s)
ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
---l_product_category---
INSERT INTO  dds.l_product_category(
hk_product_category_pk,
h_product_pk,
h_category_pk,
load_dt,
load_src)
values(
     %(hk_product_category_pk)s,
    %(h_product_pk)s,
    %(h_category_pk)s,
    %(load_dt)s,
    %(load_src)s)
ON CONFLICT (hk_product_category_pk) DO NOTHING;
---l_order_user---
INSERT INTO  dds.l_order_user(
hk_order_user_pk,
h_order_pk,
h_user_pk,
load_dt,
load_src)
values(
     %(hk_order_user_pk)s,
    %(h_order_pk)s,
    %(h_user_pk)s,
    %(load_dt)s,
    %(load_src)s)
ON CONFLICT (hk_order_user_pk) DO NOTHING;
---s_order_cost---
INSERT INTO  dds.s_order_cost (
	hk_order_cost_pk,
	h_order_pk,
	"cost",
	payment,
	load_dt,
	load_src)
    VALUES(
	%(hk_order_cost_pk)s,
	%(h_order_pk)s,
	%(cost)s,
	%(payment)s,
	%(load_dt)s,
	%(load_src)s
    )
    ON CONFLICT (hk_order_cost_pk) DO NOTHING;
    ;
---s_order_status---
INSERT INTO  dds.s_order_status (
	hk_order_status_pk,
	h_order_pk,
	"status",
	load_dt,
	load_src)
    VALUES(
	%(hk_order_status_pk)s,
	%(h_order_pk)s,
	%(status)s,
	%(load_dt)s,
	%(load_src)s
    )
    ON CONFLICT (hk_order_status_pk) DO NOTHING;
    ;
---s_product_names---
INSERT INTO  dds.s_product_names (
	hk_product_names_pk,
	h_product_pk,
	"name",
	load_dt,
	load_src)
    VALUES(
	%(hk_product_names_pk)s,
	%(h_product_pk)s,
	%(name)s,
	%(load_dt)s,
	%(load_src)s
    )
    ON CONFLICT (hk_product_names_pk) DO NOTHING;
    ;
---s_restaurant_names---
INSERT INTO  dds.s_restaurant_names (
	hk_restaurant_names_pk,
	h_restaurant_pk,
	"name",
	load_dt,
	load_src)
    VALUES(
	%(hk_restaurant_names_pk)s,
	%(h_restaurant_pk)s,
	%(name)s,
	%(load_dt)s,
	%(load_src)s
    )
    ON CONFLICT (hk_restaurant_names_pk) DO NOTHING;
    ;
---s_user_names---
INSERT INTO  dds.s_user_names (
	hk_user_names_pk,
	h_user_pk,
	username,
    userlogin,
	load_dt,
	load_src)
    VALUES(
	%(hk_user_names_pk)s,
	%(h_user_pk)s,
	%(username)s,
    %(userlogin)s,
	%(load_dt)s,
	%(load_src)s
    )
    ON CONFLICT (hk_user_names_pk) DO NOTHING;
    ;