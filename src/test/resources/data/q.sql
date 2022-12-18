truncate table aaa;
truncate table bbb;
truncate ccc;

INSERT INTO aaa (a,b,c) VALUES ('10',1,null);
INSERT INTO bbb (d,e,f) VALUES ("20",2,NULL);
INSERT INTO ccc VALUES ('20',2,NULL);
INSERT INTO ddd VALUES (20,2,NULL);;
-- INSERT INTO fff VALUES (20,2,NULL);
INSERT INTO 
    ddd VALUES (10,2,NULL);

INSERT INTO
    ddd VALUES (func(1,3),2,'{"field1":"abc","field2":["a","b"],"field3":2}');

INSERT INTO
    ddd VALUES (concat(date_format(current_timestamp() - interval 2 month, '%Y%m'),'20'),2,"{"field1":"abc","field2":["a","b"],"field3":null}");
