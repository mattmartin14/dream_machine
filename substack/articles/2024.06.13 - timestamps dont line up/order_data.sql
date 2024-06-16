create table order_hdr (order_id int, crt_ts datetime);

insert into order_hdr values
    (1, '2024-01-01 08:05:23'),
    (2, '2024-01-03 12:16:44'),
    (3, '2024-02-05 18:01:12')
;

create table order_event_dtl (order_id int, event_type int, event_desc string, event_ts datetime);

INSERT INTO order_event_dtl (order_id, event_type, event_desc, event_ts) values
    
    (1, 1, 'order_create', '2024-01-01 08:05:32'),
    (1, 2, 'order_pack', '2024-01-01 09:13:44'),
    (1, 3, 'conveyor_load', '2024-01-01 09:15:12'),
    (1, 99, 'exception', '2024-01-01 09:35:03'),
    (1, 3, 'conveyor_load', '2024-01-01 10:02:18'),
    (1, 4, 'order_shipped', '2024-01-01 18:34:16'),
    (1, 5, 'order_delivered', '2024-01-03 07:09:33'),

    (2, 1, 'order_create', '2024-01-03 08:05:32'),
    (2, 2, 'order_pack', '2024-01-03 09:13:44'),
    (2, 3, 'conveyor_load', '2024-01-03 09:15:12'),
    (2, 4, 'order_shipped', '2024-01-04 18:34:16'),
    (2, 5, 'order_delivered', '2024-01-08 07:09:33'),

    (3, 1, 'order_create', '2024-01-05 08:05:32'),
    (3, 2, 'order_pack', '2024-01-05 09:13:44'),
    (3, 3, 'conveyor_load', '2024-01-05 09:15:12'),
    (3, 4, 'order_shipped', '2024-01-05 18:34:16'),
    (3, 99, 'exception', '2024-01-06 08:12:02'),
    (3, 2, 'order_pack', '2024-01-06 09:10:13'),
    (3, 3, 'conveyor_load', '2024-01-06 10:05:04'),
    (3, 99, 'exception', '2024-01-06 10:08:11'),
    (3, 3, 'conveyor_load', '2024-01-06 10:15:22'),
    (3, 4, 'order_shipped', '2024-01-06 18:22:02'),
    (3, 5, 'order_delivered', '2024-01-10 07:09:33')
;

create table order_event_ex (order_id int, except_ts datetime, ex_desc string);

INSERT INTO order_event_ex (order_id, except_ts, ex_desc) values 
(1, '2024-01-01 09:37:05', 'missing label'),
(3, '2024-01-06 08:15:05', 'order lost - creating new one'),
(3, '2024-01-06 10:10:16', 'package fell off conveyor')
;
