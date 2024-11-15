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

create table local_msg_test.local_msgs
(
    id         bigint auto_increment
        primary key,
    `key`      varchar(191)     null,
    data       TEXT             null,
    send_times bigint           null,
    status     tinyint unsigned null,
    utime      bigint           null,
    ctime      bigint           null
);

create index idx_local_msgs_key
    on local_msg_test.local_msgs (`key`);

create index utime_status
    on local_msg_test.local_msgs (status, utime);

-- 下面这些用来测试分库分表
-- 分成两个库
CREATE DATABASE IF NOT EXISTS `orders_db_00`;
CREATE DATABASE IF NOT EXISTS `orders_db_01`;

CREATE TABLE `orders_db_00`.orders_tab_00 LIKE `local_msg_test`.orders;
CREATE TABLE `orders_db_00`.orders_tab_01 LIKE `local_msg_test`.orders;

CREATE TABLE `orders_db_01`.orders_tab_00 LIKE `local_msg_test`.orders;
CREATE TABLE `orders_db_01`.orders_tab_01 LIKE `local_msg_test`.orders;

CREATE TABLE `orders_db_00`.local_msgs_tab_00 LIKE `local_msg_test`.local_msgs;
CREATE TABLE `orders_db_00`.local_msgs_tab_01 LIKE `local_msg_test`.local_msgs;

CREATE TABLE `orders_db_01`.local_msgs_tab_00 LIKE `local_msg_test`.local_msgs;
CREATE TABLE `orders_db_01`.local_msgs_tab_01 LIKE `local_msg_test`.local_msgs;