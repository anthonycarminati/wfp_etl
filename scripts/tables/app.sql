create table lu_user(
	id serial
	,email character varying(64)
	,username character varying(64)
	,is_admin boolean
	,password_hash character varying(128)
	,name character varying(64)
);