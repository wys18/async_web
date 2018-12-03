DROP database if EXISTS awesome_web;
CREATE DATABASE awesome_web;
USE awesome_web;

CREATE TABLE users(
    `id` VARCHAR(50) NOT NULL,
    `email` VARCHAR(50) NOT NULL,
    `passwd` VARCHAR(50) NOT NULL
)