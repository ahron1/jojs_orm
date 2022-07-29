-module(db_helpers).

-export([id_given_login_pw/2, id_given_login/1, id_given_handle/1, id_given_fingerprint/1, get_sys_user/0, name_given_id/1, active_status_given_id/1, is_invited_given_id/1, is_fresh_given_id/1, update_name/2, is_waiting_over/1, new_guest_creation/1, spot_account_creation/3, new_account_creation/2, create_signup_token/1, id_given_signup_token/1, activate_new_account/1]). 
-export([id_given_cookie/1, create_session_cookie/1, delete_session_cookie/1, cookie_given_id/1, log_signin/2, log_signout/1]). 
-export([create_pw_token/1, id_given_valid_pw_token/1, update_pw/2, activate_pw_token/1, check_valid_pw_token/1, disable_pw_token/1]).
-export([image_details_to_db/6, image_details_to_db_special/6, get_new_pics/1, get_this_pic/1, record_votes/5]).
-export([update_vetted_adj/0]).
-export([testing/1]).

% %% account/id/pw
%% get user id given submitted id/pw for logging in
id_given_login_pw(FormLogin, FormPassword) ->
	{{select, N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_credentials where email=$1 and password_text=crypt($2, password_text)", [FormLogin, FormPassword]),
	{{select, N}, IdTupleList}.

%% get user id given submitted login id (check if login id is valid)
id_given_login(FormLogin) ->
	{{select, N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_credentials where email=$1", [FormLogin]),
	{{select, N}, IdTupleList}.

%% get user id given handle
id_given_handle(Handle) ->
	{{select, N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_credentials where handle=$1", [Handle]),
	{{select, N}, IdTupleList}.

%% get user id given fingerprint
id_given_fingerprint(FingerPrint) ->
	{{select, N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_guest where fingerprint=$1", [FingerPrint]),
	{{select, N}, IdTupleList}.

%% get random system user id (for uploading system pics)
get_sys_user() ->
	{{select, 1}, IdTupleList} = pp_db:simple_query("select property_of from person_property_credentials where is_system_user=true order by random() limit 1"),
	IdTupleList.

%% get user name given id
name_given_id(Id) -> 
	{{select, 1}, NameTupleList} = pp_db:extended_query("select name from person_property where property_of = $1", [Id]),
	[{Name}] = NameTupleList,
	Name. 

%% check if user is present in person_property_acquisition_invited db
is_invited_given_id(Id) ->
	{{select, N}, _} = pp_db:extended_query("select id from person_property_acquisition_invited where property_of = $1", [Id]),
	case N of 
		1 ->
			true;
		0 ->
			false 
	end.

%% check if user is present in person_property_acquisition_fresh db
is_fresh_given_id(Id) ->
	{{select, N}, _} = pp_db:extended_query("select id from person_property_acquisition_fresh where property_of = $1", [Id]),
	case N of 
		1 ->
			true;
		0 ->
			false
	end.

%% get account activation status from the database given the submitted login
active_status_given_id(Id) ->
	{{select,N}, ActiveStatusTupleList} = pp_db:extended_query("select is_account_active from person_property_credentials where property_of = $1", [Id]),
	{{select,N}, ActiveStatusTupleList}.

%% update first name in db
update_name(Id, Name) ->
	{{update, 1}, X} = pp_db:extended_query("update person_property set name = $2 where property_of=$1", [Id, Name]),
	{{update, 1}, X}.

%% is waiting period over - return true(waiting period is over)/false(waiting period is not over)/null(no record for this ID)
is_waiting_over(Id) ->
	{{select, N}, TFTuple} = pp_db:extended_query("select (select current_timestamp - end_of_waiting_period from person_property_acquisition_fresh where property_of = $1) > interval '0'", [Id]),
	{{select, N}, TFTuple}.

%% new guest account creation using given fingerprint
new_guest_creation(FingerPrint) ->
	erlang:display(in_db_helper_new_guest_creation),
	{{select, N}, NewUserIdTupleList} = pp_db:extended_query("
		with 
			person as (
			insert into person 
			values(default) 
			returning id
		    )
 		    ,person_property as (
			insert into person_property (property_of ) 
			select id from person 
			returning id
			)
			,person_property_guest as (
			insert into person_property_guest (property_of, fingerprint) 
			select id, (select $1) from person
			returning id
			)
		select id from person
		", [FingerPrint]),
	{{select, N}, NewUserIdTupleList}.

%% new spot account creation using given login/pw/name
spot_account_creation(Login, Password, Name) ->
	{{select, N}, NewUserIdTupleList} = pp_db:extended_query("
		with 
			person as (
			insert into person 
			values(default) 
			returning id
		    )
 		    ,person_property as (
			insert into person_property (property_of, name) 
			select id, (select $3) from person 
			returning id
			)
			,person_property_credentials as (
			insert into person_property_credentials (property_of, email, password_text, is_account_active) 
			select id, (select $1), (select crypt($2, gen_salt('bf', 8))), true from person
			returning id
			)
		select id from person
		", [Login, Password, Name]),
	{{select, N}, NewUserIdTupleList}.

%% new (inactive) account creation using given login/name
%% adjust hardcoded value in last expression to change waiting time
new_account_creation(Login, Name) ->
	{{select, N}, NewUserIdTupleList} = pp_db:extended_query("
		with 
			person as (
			insert into person 
			values(default) 
			returning id
		    )
 		    ,person_property as (
			insert into person_property (property_of, name) 
			select id, (select $2) from person 
			returning id
			)
			,person_property_credentials as (
			insert into person_property_credentials (property_of, email) 
			select id, (select $1) from person
			returning id
			)
			,person_property_acquisition_fresh as (
			insert into person_property_acquisition_fresh (property_of, end_of_waiting_period)
			select id, (current_timestamp + '0 days') from person
			returning id
			)
		select id from person
		", [Login, Name]),
	{{select, N}, NewUserIdTupleList}.

%% create and return new signup token
create_signup_token(Id) -> 
	{{select, N}, TokenTupleList} = pp_db:extended_query("
		with 
			person_property_tokens_signup as (
			insert into person_property_tokens_signup (property_of)
			select $1
			on conflict (property_of)
			do 
			    update set
					token_value = encode(hmac(gen_random_uuid()::text, gen_salt('md5'::text), 'sha256'::text), 'hex'::text)
					,time_of_creation = current_timestamp
			returning token_value
			)
		select token_value from person_property_tokens_signup
		", [Id]),
	{{select, N}, TokenTupleList}.

%% signup token validation
%% todo - also check if the token is valid pp_tokens_signup.is_signup_token_valid  - table/inline function. the function exists but is not used currently. so unused signup tokens are indefinitely valid. potential security risk? 
id_given_signup_token(JoinToken) ->
	{{select,N}, IdActTupleList} = pp_db:extended_query("select property_of, is_token_activated from person_property_tokens_signup where token_value = $1", [JoinToken]),
	{{select, N}, IdActTupleList}.

%% account activation
activate_new_account(Id) ->
	%todo: below transactions should be made atomic with setting of password
	{{update, 1}, _} = pp_db:extended_query("update person_property_credentials set is_account_active = true where property_of=$1", [Id]),
	{{update, 1}, _} = pp_db:extended_query("update person_property_tokens_signup set is_token_activated = true where property_of=$1", [Id]),
	ok.

% %% cookies
%%check cookie in db given ID. if user closed window losing still valid cookie 
cookie_given_id(Id) ->
	{{select,N}, CookieTupleList} = pp_db:extended_query("select cookie from person_property_session p where property_of = $1 and p.is_cookie_valid = true", [Id]),
	%update last update timestamp if cookie exists
	case N of 
		1 -> 
			{{update, 1}, _} = pp_db:extended_query("update person_property_session set time_of_last_update = CURRENT_TIMESTAMP where property_of=$1", [Id]);
		_ ->
			no_ongoing_session
	end,
	{N, CookieTupleList}.

%% check in db if cookie exists. Get Id given cookie and update cookie timestamp
%check_session_cookie(Cookie) ->
id_given_cookie(Cookie) ->
	%add something like where is_valid=1 to query to check only for valid cookies
	{{select,N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_session p where cookie = $1 and p.is_cookie_valid = true", [Cookie]),
	case N of 
		1 -> 
			%update last update timestamp if cookie exists
			[{Id}] = IdTupleList,
			erlang:display(Id),
			{{update, 1}, _} = pp_db:extended_query("update person_property_session set time_of_last_update = CURRENT_TIMESTAMP where property_of=$1", [Id]),
			{has_session_cookie, Id};
		_ ->
			{has_no_session_cookie, no_such_user}
	end.

%% insert login id into cookie table returning value of generated cookie
create_session_cookie(Id) ->
	%first delete any old (invalid) cookie for the userid then insert
	{{delete, _}, _} = delete_session_cookie(Id),
	%{{insert, 0, 1}, [{CookieValue}]} = pp_db:extended_query("insert into person_property_session(property_of) values($1) returning cookie", [Id]),
	{{insert, 1}, [{CookieValue}]} = pp_db:extended_query("insert into person_property_session(property_of) values($1) returning cookie", [Id]),
	CookieValue.

%% delete session cookie given Id
delete_session_cookie(Id) ->
	{{delete, _}, _} = pp_db:extended_query("delete from person_property_session where property_of=$1", [Id]).

%%create new log entry for user sign in
log_signin(UserId, Cookie) ->
	{{insert, 1}, _} = pp_db:extended_query("insert into person_property_session_log(property_of, session_id) values($1, $2) returning id",[UserId, Cookie]),
	ok.

%%update log entry for user sign out
log_signout(SessionId) ->
	{{update, 1}, _} = pp_db:extended_query("update person_property_session_log set logout_timestamp = CURRENT_TIMESTAMP where session_id = $1", [SessionId]),
	ok.

% %% password reset
%%insert new token for resetting pasword
create_pw_token(Id) ->
	%first delete any old token for the userid then insert
	{{delete, _}, _} = pp_db:extended_query("delete from person_property_tokens_pw where property_of = $1", [Id]),
	%{{insert, 0, 1}, _} = 	pp_db:extended_query("insert into person_property_tokens_pw(property_of) values($1) returning token_value", [Id]).
	{{insert, 1}, _} = 	pp_db:extended_query("insert into person_property_tokens_pw(property_of) values($1) returning token_value", [Id]).

%%check if password token is valid and return id if it is
id_given_valid_pw_token(Token) ->
	%erlang:display(in_id_given_pw_token_db),
	%the is_token_activated field is to ensure the user has actually clicked on the email link, and it is not a malicious user somehow sending the form.
	%check for is_token_activated because user might click on pw reset link but not actually change the pw. so the link can be clicked only once. 
	%also check for is_token_used to prevent reuse/abuse of token
	{{select,N}, IdTupleList} = pp_db:extended_query("select property_of from person_property_tokens_pw p where token_value = $1 and is_token_used = false and is_token_activated = false and p.is_pw_token_valid = true", [Token]),
	%it is possible to call the function check_valid_pw_token, but better to have a single query and add a condition to it. 
	%erlang:display(IdTupleList),
	{{select,N}, IdTupleList}.

%%update password given login
update_pw(Id, NewPassword) ->
	{{update, 1}, _} = pp_db:extended_query("update person_property_credentials set password_text = (crypt($1, gen_salt('bf', 8))) where property_of=$2", [NewPassword, Id]).

%%activate password token given login
activate_pw_token(Id) ->
	{{update, 1}, _} = pp_db:extended_query("update person_property_tokens_pw set is_token_activated = true where property_of=$1", [Id]).

%%check if there is valid and activated pw token
check_valid_pw_token(Id) ->
	{{select, N}, X} = pp_db:extended_query("select 1 from person_property_tokens_pw p where property_of=$1 and is_token_used = false and is_token_activated = true and p.is_pw_token_valid = true", [Id]),
	{{select, N}, X}.

%%set is_token_used to true to prevent reuse/abuse
disable_pw_token(Id) ->
	{{update, 1}, _} = pp_db:extended_query("update person_property_tokens_pw set is_token_used = true where property_of=$1", [Id]).

% %% imagery
%% save image details to db via upload_handler_special. 
image_details_to_db_special(UserId, NewFileName, PicUri, CompletePath, Adj1, Adj2) ->
	{{select, 1}, [{NewImageId, Adj1Text, Adj2Text}]} = pp_db:extended_query("
		with 
		   -- input details from server
		   userid as (
		   select ($1::int) as id
		   )
		   ,picfilename as ( 
		   select ($2) as filename
		   )
		   ,picuri as ( 
		   select ($3) as uri
		   )
		   ,completepath as ( 
		   select ($4) as path
		   )
		   ,inputword1 as ( 
		   select ($5::citext) as inputword
		   )
		   ,inputword2 as (
		   select ($6::citext) as inputword
		   )
		   ,syspic_bool as (
		   select ('true'::boolean) as tf
		   )
--		   ,active_pic_bool as (
--		   select ('true'::boolean) as tf
--		   )
	   -- check conditions and insert into db
		   ,w1 as ( 
		   select word, property_of
		   from   adjective_property
		   where  word = (select inputword from inputword1)
		   )
		   ,a1 as (
		   insert into adjective            
		   select                          
		   where not exists (select from w1)
		   returning id
		   )
		   ,ap1 as (
		   insert into adjective_property (word, property_of) 
		   select (select inputword from inputword1), id
		   from   a1
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
		   returning word, property_of
		   )
		   ,w2 as (
		   select word, property_of
		   from   adjective_property
		   where  word = (select inputword from inputword2)
		   )
		   ,a2 as (
     	   insert into adjective            
   		   select                          
		   where not exists (select from w2)
		   returning id
		   )
		   ,ap2 as (
		   insert into adjective_property (word, property_of) 
		   select (select inputword from inputword2), id
		   from   a2
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
		   returning word, property_of
		   ) 
		   ,p as ( 
		   insert into picture
   		   values(default)
   		   returning id
 		   )     
 		   ,pp as (
 		   insert into picture_property(property_of, filename, picture_uri, complete_path, is_system_picture)
 		   select id, (select filename from picfilename), (select uri from picuri), (select path from completepath), (select tf from syspic_bool) from p
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
 		   )
 		   ,person2p as (
 		   insert into person_to_picture(source, target)
 		   select (select id from userid), id from p
 		   returning id
 		   )
 		   ,person2pp as ( 
 		   insert into person_to_picture_property(property_of, is_uploader)
 		   select id, 'true' from person2p
		   )
		   ,a2p1 as ( 
		   insert into adjective_to_picture(source, target) 
		   values ((select property_of from ap1 union all select property_of from w1), (select id from p))
		   returning id, target, source 
		   )
		   ,a2pp1 as (
		   insert into adjective_to_picture_property(property_of, coupled_by) 
		   select id, (select id from userid) from a2p1
		   )
		   ,a2p2 as (
		   insert into adjective_to_picture(source, target) 
		   values ((select property_of from ap2 union all select property_of from w2), (select id from p))
		   returning id, target, source
		   )
		   ,a2pp2 as (
		   insert into adjective_to_picture_property(property_of, coupled_by) 
		   select id, (select id from userid) from a2p2
		   )
--	  	   ,a2a_new as (
--		   insert into adjective_to_adjective(source, target)
--		   values ( 
--				(select property_of from w1 union all select property_of from ap1) 
--				,(select property_of from w2 union all select property_of from ap2)
--				)
--		   on conflict  
--		   do nothing 
--		   returning source, target, id
--		   )
--		   ,a2ap as ( 
--		   insert into adjective_to_adjective_property (property_of, is_pair)
--		   select id, 'true' from a2a_new
--		   where exists (select 1 from a2a_new) 
--		   returning id, property_of
--		   )
		   ,person2a1 as ( 
		   insert into person_to_adjective(source, target) 
		   select (select id from userid), property_of from ap1
		   where exists (select 1 from ap1) 
		   returning id, target, source
		   )
		   ,person2a1p as (
		   insert into person_to_adjective_property(property_of, is_uploader) 
		   select id, 'true' from person2a1
		   where exists (select 1 from person2a1)
		   )    
		   ,person2a2 as (
		   insert into person_to_adjective(source, target) 
		   select (select id from userid), property_of from ap2
		   where exists (select 1 from ap2)
		   returning id, target, source
		   )
		   ,person2a2p as ( 
		   insert into person_to_adjective_property(property_of, is_uploader) 
		   select id, 'true' from person2a2
		   where exists (select 1 from person2a2)
		   )    
		   -- join tables and select output
	    select p.id as pic, w1.word as adj1, w2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join w1 on w1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join w2 on w2.property_of = a2p2.source

		union all 

		select p.id as pic, ap1.word as adj1, ap2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join ap1 on ap1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join ap2 on ap2.property_of = a2p2.source

		union all 

		select p.id as pic, w1.word as adj1, ap2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join w1 on w1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join ap2 on ap2.property_of = a2p2.source

		union all 

		select p.id as pic, ap1.word as adj1, w2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join ap1 on ap1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join w2 on w2.property_of = a2p2.source
	", [UserId, NewFileName, PicUri, CompletePath, Adj1, Adj2 ]),
	{NewImageId, Adj1Text, Adj2Text}.

%% save image details to db via webform
%% difference with previous fun is the hardcoded value of syspic_bool
image_details_to_db(UserId, NewFileName, PicUri, CompletePath, Adj1, Adj2) ->
	{{select, 1}, [{NewImageId, Adj1Text, Adj2Text}]} = pp_db:extended_query("
		with 
		   -- input details from server
		   userid as (
		   select ($1::int) as id
		   )
		   ,picfilename as ( 
		   select ($2) as filename
		   )
		   ,picuri as ( 
		   select ($3) as uri
		   )
		   ,completepath as ( 
		   select ($4) as path
		   )
		   ,inputword1 as ( 
		   select ($5::citext) as inputword
		   )
		   ,inputword2 as (
		   select ($6::citext) as inputword
		   )
		   ,syspic_bool as (
		   select ('false'::boolean) as tf
		   )
	   -- check conditions and insert into db
		   ,w1 as ( 
		   select word, property_of
		   from   adjective_property
		   where  word = (select inputword from inputword1)
		   )
		   ,a1 as (
		   insert into adjective            
		   select                          
		   where not exists (select from w1)
		   returning id
		   )
		   ,ap1 as (
		   insert into adjective_property (word, property_of) 
		   select (select inputword from inputword1), id
		   from   a1
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
		   returning word, property_of
		   )
		   ,w2 as (
		   select word, property_of
		   from   adjective_property
		   where  word = (select inputword from inputword2)
		   )
		   ,a2 as (
     	   insert into adjective            
   		   select                          
		   where not exists (select from w2)
		   returning id
		   )
		   ,ap2 as (
		   insert into adjective_property (word, property_of) 
		   select (select inputword from inputword2), id
		   from   a2
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
		   returning word, property_of
		   ) 
		   ,p as ( 
		   insert into picture
   		   values(default)
   		   returning id
 		   )     
 		   ,pp as (
 		   insert into picture_property(property_of, filename, picture_uri, complete_path, is_system_picture)
 		   select id, (select filename from picfilename), (select uri from picuri), (select path from completepath), (select tf from syspic_bool) from p
		   on conflict  
		   do nothing  --on duplicate file (name) do nothing (fail silently)
 		   )
 		   ,person2p as (
 		   insert into person_to_picture(source, target)
 		   select (select id from userid), id from p
 		   returning id
 		   )
 		   ,person2pp as ( 
 		   insert into person_to_picture_property(property_of, is_uploader)
 		   select id, 'true' from person2p
		   )
		   ,a2p1 as ( 
		   insert into adjective_to_picture(source, target) 
		   values ((select property_of from ap1 union all select property_of from w1), (select id from p))
		   returning id, target, source 
		   )
		   ,a2pp1 as (
		   insert into adjective_to_picture_property(property_of, coupled_by) 
		   select id, (select id from userid) from a2p1
		   )
		   ,a2p2 as (
		   insert into adjective_to_picture(source, target) 
		   values ((select property_of from ap2 union all select property_of from w2), (select id from p))
		   returning id, target, source
		   )
		   ,a2pp2 as (
		   insert into adjective_to_picture_property(property_of, coupled_by) 
		   select id, (select id from userid) from a2p2
		   )
--	  	   ,a2a_new as (
--		   insert into adjective_to_adjective(source, target)
--		   values ( 
--				(select property_of from w1 union all select property_of from ap1) 
--				,(select property_of from w2 union all select property_of from ap2)
--				)
--		   on conflict  
--		   do nothing 
--		   returning source, target, id
--		   )
--		   ,a2ap as ( 
--		   insert into adjective_to_adjective_property (property_of, is_pair)
--		   select id, 'true' from a2a_new
--		   where exists (select 1 from a2a_new) 
--		   returning id, property_of
--		   )
		   ,person2a1 as ( 
		   insert into person_to_adjective(source, target) 
		   select (select id from userid), property_of from ap1
		   where exists (select 1 from ap1) 
		   returning id, target, source
		   )
		   ,person2a1p as (
		   insert into person_to_adjective_property(property_of, is_uploader) 
		   select id, 'true' from person2a1
		   where exists (select 1 from person2a1)
		   )    
		   ,person2a2 as (
		   insert into person_to_adjective(source, target) 
		   select (select id from userid), property_of from ap2
		   where exists (select 1 from ap2)
		   returning id, target, source
		   )
		   ,person2a2p as ( 
		   insert into person_to_adjective_property(property_of, is_uploader) 
		   select id, 'true' from person2a2
		   where exists (select 1 from person2a2)
		   )    
		   -- join tables and select output
	    select p.id as pic, w1.word as adj1, w2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join w1 on w1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join w2 on w2.property_of = a2p2.source

		union all 

		select p.id as pic, ap1.word as adj1, ap2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join ap1 on ap1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join ap2 on ap2.property_of = a2p2.source

		union all 

		select p.id as pic, w1.word as adj1, ap2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join w1 on w1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join ap2 on ap2.property_of = a2p2.source

		union all 

		select p.id as pic, ap1.word as adj1, w2.word as adj2 
		from p
		join a2p1 on p.id = a2p1.target
		join ap1 on ap1.property_of = a2p1.source
		join a2p2 on p.id = a2p2.target
		join w2 on w2.property_of = a2p2.source
	", [UserId, NewFileName, PicUri, CompletePath, Adj1, Adj2 ]),
	{NewImageId, Adj1Text, Adj2Text}.

%% get list of next 5 pics (with adjectives) not seen by given user
%% todo - additionally return session.pics array size to erlang code. if too large, reset array to empty. 
get_new_pics(UserId) ->
	{{select, _N}, ImgAdjTupleList} = pp_db:extended_query("
	with 
		person as (
		select ($1::int) as id
		)
		,adjs_temp as (
		select w1_adj, w1_prop, w2_adj, w2_prop--, row_number() over (order by w1_prop) 
		from _mv_adj_pair_list
		order by random()
		limit 5
		)
		,pics_temp as (
		select picture_uri, property_of as pic_id--, row_number() over (order by picture_uri) 
		from picture_property
		where 
-- the next lines were to ensure that pics were not repeated in the same session
-- they are skipped due to being a potential memory overflow risk
-- todo - test and profile before re-enabling
--			property_of <> all (
--			(select pics from person_property_session 
--			where property_of = (select id from person))::int[])
--		and is_system_picture = false
		is_picture_active = true
		order by random()
		limit 5
		)
		,adjs as (
		select w1_adj, w1_prop, w2_adj, w2_prop, row_number() over (order by w1_prop) 
		from adjs_temp
		)
		,pics as (
		select picture_uri, pic_id, row_number() over (order by pic_id) 
		from pics_temp
		)
		,paa as (
		select  pic_id, w1_adj, w1_prop, w2_adj, w2_prop, picture_uri
		from pics as xx
		join 
		adjs as yy
		on xx.row_number = yy.row_number
		)
-- the next lines were to ensure that pics were not repeated in the same session
--		,sessionpics as (
--		update person_property_session 
--		set pics =  pics || (select array_agg(pic_id) from paa)
--		where (property_of = (select id from person))
--		)
	select * from paa
	", [UserId]),
	%erlang:display(ImgAdjTupleList),
	ImgAdjTupleList.

%% get details of one specific pic
get_this_pic(PicId) ->
	{{select, _N}, ImgAdjTupleList} = pp_db:extended_query("
	with 
		picid as ( --get from client/server code
		select ($1::int) as id
		)
		,adjs_temp as (
		select w1_adj, w1_prop, w2_adj, w2_prop--, row_number() over (order by w1_prop) 
		from _mv_adj_pair_list
		order by random()
		limit 1
		)
		,pics_temp as (
		select picture_uri, property_of as pic_id--, row_number() over (order by picture_uri) 
		from picture_property
		where property_of = (select id from picid)
		order by random()
		limit 1
		)
		,adjs as (
		select w1_adj, w1_prop, w2_adj, w2_prop, row_number() over (order by w1_prop) 
		from adjs_temp
		)
		,pics as (
		select picture_uri, pic_id, row_number() over (order by pic_id) 
		from pics_temp
		)
		,paa as (
		select  pic_id, w1_adj, w1_prop, w2_adj, w2_prop, picture_uri
		from pics as xx
		join 
		adjs as yy
		on xx.row_number = yy.row_number
		)
	select * from paa
	", [PicId]),
	%erlang:display(ImgAdjTupleList),
	ImgAdjTupleList.

%% write votes to db
record_votes(Adj1, Adj2, VoteChoice, PicId, UserId) ->
	pp_db:extended_query("
		with
			adj1 as ( --get from client/server code
				select ($1::int) as id
				)
			,adj2 as ( --get from client/server code
				select ($2::int) as id
				)
			,vote_choice as (--get from client/server code
				select ($3::smallint) as vote
				)	
			,picture as ( --get from client/server code
				select ($4::int) as id
				)
			,userid as ( --get from client/server code
				select ($5::int) as id
				)
			,person2pic as ( --either person to pic edge exists
				select id 
				from person_to_picture
				where 
					source = (select id from userid)
						and
					target = (select id from picture)
				)
			,person2pic_new as ( --or person to pic edge will be created
				insert into person_to_picture(source, target)
				select (select id from userid), id 
				from picture
				where not exists (select from person2pic) --if person to pic edge doesn't already exist
				returning id
				)
			,person2pic_vote as ( -- to check if the vote for this specific combination (person, pic, adj1/2) already exists
				select id from person_to_picture_property_voting_log
				where 
				property_of = (select id from person2pic) --only existing edge prop needs to be checked, since a new edge wouldn't have it anyway
					and 
					(((adjective_1 = (select id from adj1)) and (adjective_2 = (select id from adj2))) 
						or ((adjective_1 = (select id from adj2)) and (adjective_2 = (select id from adj1)))
						)
				)
			-- it is not (yet) being checked if the adj/pic are indeed linked. it is assumed the server/client are sending good info
		    insert into person_to_picture_property_voting_log (adjective_1, adjective_2, property_of, vote_choice)
				select id
				,(select id from adj2)
				,((select id from person2pic) union all (select id from person2pic_new)) -- this union implies the uploader can vote on their own pic
				-- to disable uploader from voting on own pic, use only from person2pic_new above (to ensure only newly created edge is used) 
				-- the is_uploader field might/not need to be checked before inserting.
				,(select vote from vote_choice)
				from adj1
			where not exists (select from person2pic_vote) -- check the vote for this specific combination (person, pic, adj1/2) doesnt already exist
			returning id
	", [Adj1, Adj2, VoteChoice, PicId, UserId]).

% %% maintenance and updates
%%update adjective active status based on vetted status.  
%is_adjective_active default and is_adjective_vetted both default to null. 
%new pic/adj insertion query doesnt change this
%manually set vetted to false for unacceptable adjs. then run this func. 
%create materialized_view checks only for is_adjective_active and not is_adjective_vetted

% since pgo doesn't support prepared statements, use a dummy cte to execute all the update transactions together.
update_vetted_adj() ->
	pp_db:simple_query("
		with  
			x as ( --dummy variable 
					select (10) as y
				)
			,a as (
				update adjective_property set is_adjective_active = true where is_adjective_active IS NULL 
				)
			,b as (
				update adjective_property set is_adjective_active = false where is_adjective_active <> false and is_adjective_vetted = false
				)
			,c as (
				update adjective_property set is_adjective_active = true where is_adjective_active = false and is_adjective_vetted = true 
				)
			select y from x
		").

%if it is necessary to use quoted column names escape the quotes as below:
%{{select,N}, UserId} = pp_db:extended_query("select \"userid\" from cookietable where value = $1", [Cookie]),

% %% testing dummies
testing(UserId) ->
	{{select, _N}, ImgAdjTupleList} = pp_db:extended_query("
	with 
		person as (
		select ($1::int) as id
		)
		,adjs_temp as (
		select w1_adj, w1_prop, w2_adj, w2_prop--, row_number() over (order by w1_prop) 
		from _mv_adj_pair_list
		order by random()
		limit 5
		)
		,pics_temp as (
		select picture_uri, property_of as pic_id--, row_number() over (order by picture_uri) 
		from picture_property
		where 
-- the next lines were to ensure that pics were not repeated in the same session
-- they are skipped due to being a potential memory overflow risk
-- todo - test and profile before re-enabling
--			property_of <> all (
--			(select pics from person_property_session 
--			where property_of = (select id from person))::int[])
--		and is_system_picture = false
		is_picture_active = true
		order by random()
		limit 5
		)
		,adjs as (
		select w1_adj, w1_prop, w2_adj, w2_prop, row_number() over (order by w1_prop) 
		from adjs_temp
		)
		,pics as (
		select picture_uri, pic_id, row_number() over (order by pic_id) 
		from pics_temp
		)
		,paa as (
		select  pic_id, w1_adj, w1_prop, w2_adj, w2_prop, picture_uri
		from pics as xx
		join 
		adjs as yy
		on xx.row_number = yy.row_number
		)
-- the next lines were to ensure that pics were not repeated in the same session
--		,sessionpics as (
--		update person_property_session 
--		set pics =  pics || (select array_agg(pic_id) from paa)
--		where (property_of = (select id from person))
--		)
	select * from paa
	", [UserId]),
	%erlang:display(ImgAdjTupleList),
	ImgAdjTupleList.

