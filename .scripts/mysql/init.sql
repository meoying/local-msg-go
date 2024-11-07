-- create the databases
CREATE DATABASE IF NOT EXISTS `local_msg_test`;

create table local_msg_test.orders
(
    id    bigint auto_increment
        primary key,
    sn    longtext null,
    utime bigint   null,
    ctime bigint   null,
    buyer bigint   null
);

-- 下面这些用来测试分库分表
-- 分成两个库
CREATE DATABASE IF NOT EXISTS `orders_db_00`;
CREATE DATABASE IF NOT EXISTS `orders_db_01`;

CREATE TABLE `orders_db_00`.orders_tab_00 LIKE `local_msg_test`.orders;
CREATE TABLE `orders_db_00`.orders_tab_01 LIKE `local_msg_test`.orders;

CREATE TABLE `orders_db_01`.orders_tab_00 LIKE `local_msg_test`.orders;
CREATE TABLE `orders_db_01`.orders_tab_01 LIKE `local_msg_test`.orders;