CREATE TABLE IF NOT EXISTS public.subscribers_restaurants(
id serial,
client_id varchar null,
restaurant_id varchar null
);
-- inserting dummy data to table subsriber_restaurants
insert into public.subscribers_restaurants (
    client_id, restaurant_id
)
values

('223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('f1c54d15-5013-4a89-83dd-aa9946529950', 'aabf4b89-2081-4191-bff8-9b22a1ae80f8'),
('4d329cae-8002-4632-aa27-434aeac96b5c', 'ff2950ec-0d3c-44bf-b309-43ccca3e04d6'),
('f86377c5-19f1-4bb3-997d-570356bc260a', 'f8ac226c-acbf-43a5-9ade-221d9a54e4ff'),
('4df86a05-59c9-4f90-8fed-b4934ecac9c3', '2b775655-9fa2-4f53-a274-d14ab86affa9'),
('21b06fae-f6d7-447d-a75a-e2f9adc709e9', 'b3a0704e-3e3c-49fc-ad99-d9bad5c220a8'),
('b626b568-e68b-4275-8049-d7f4bafda2f3', 'e49168d6-169b-47e1-8a9e-05c38c34c206'),
('10d2973d-3622-4df9-8b3e-c257dbb59fd5', '12200285-3f50-4e7d-b651-adfe5c2c734c'),
('ecc09f13-da91-4d5d-b37b-0f75e75cc5a5', '8b47761f-840b-444a-bd10-8fb01b9922fa'),
('51f5b8ef-44d6-4aa8-8e32-aefdbbb2ca0b', 'def00dcc-86a8-48a7-8a99-516988177a17'),
('c7e0c69e-94fa-4adf-a7a8-6bcac792d2ec', 'eb10d94a-f4a1-437c-ad62-106b43053a3b')
;
