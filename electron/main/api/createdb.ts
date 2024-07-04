const createTableText = `
CREATE TABLE IF NOT EXISTS hwconfig (
  name text PRIMARY KEY NOT NULL,
  data JSONB
);
CREATE TABLE IF NOT EXISTS tags (
  tag JSONB PRIMARY KEY NOT NULL,
  val NUMERIC,
  updated TIMESTAMPTZ not null default current_timestamp,
  link BOOLEAN
);
CREATE INDEX IF NOT EXISTS idxgingroup ON tags USING gin ((tag -> 'group'));
CREATE INDEX IF NOT EXISTS idxginname ON tags USING gin ((tag -> 'name'));
CREATE INDEX IF NOT EXISTS idxgindev ON tags USING gin ((tag -> 'dev'));
CREATE INDEX IF NOT EXISTS idxgintype ON tags USING gin ((tag -> 'type'));
CREATE INDEX IF NOT EXISTS idxginreg ON tags USING gin ((tag -> 'reg'));
DROP TABLE IF EXISTS locales;
CREATE TABLE locales (
  locale text PRIMARY KEY NOT NULL,
  translation JSONB,
  selected BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name text not null,
    email text,
    phonenumber text,
    password varchar not null,
    role text
  );
CREATE TABLE IF NOT EXISTS modelog (
    timestamp tstzrange not null default tstzrange(current_timestamp,NULL,'[)'),
    modecode NUMERIC not null,
    picks NUMERIC,
    planspeed NUMERIC default NULL,
    plandensity NUMERIC default NULL,
    realpicks NUMERIC
  );
CREATE INDEX IF NOT EXISTS modelog_tstzrange_idx ON modelog USING GIST (timestamp);
CREATE INDEX IF NOT EXISTS modelog_tstzrange_lower_idx ON modelog USING btree (lower(timestamp));
CREATE INDEX IF NOT EXISTS modelog_tstzrange_upper_idx ON modelog USING btree (upper(timestamp));
CREATE INDEX IF NOT EXISTS modelog_tstzrange_upperinf_idx ON modelog USING btree (upper_inf(timestamp));
CREATE TABLE IF NOT EXISTS userlog (
  timestamp tstzrange not null default tstzrange(current_timestamp,NULL,'[)'),
  id NUMERIC,
  name text not null,
  role text,
  loginby text,
  logoutby text
);
CREATE INDEX IF NOT EXISTS userlog_tstzrange_idx ON userlog USING GIST (timestamp);
CREATE TABLE IF NOT EXISTS lifetime (
  type text,
  serialno text PRIMARY KEY,
  mfgdate date,
  picks NUMERIC not null default 0,
  cloth NUMERIC not null default 0,
  motor interval not null default interval '0' second
);
DROP RULE IF EXISTS lifetime_del_protect ON lifetime;
CREATE RULE lifetime_del_protect AS ON DELETE TO lifetime DO INSTEAD NOTHING;
CREATE TABLE IF NOT EXISTS shiftconfig (
    shiftname text PRIMARY KEY not null,
    starttime TIMETZ(0),
    duration interval,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN
  );
CREATE TABLE IF NOT EXISTS clothlog (
    timestamp tstzrange PRIMARY KEY not null default tstzrange(current_timestamp,NULL,'[)'),
    event NUMERIC,
    meters numeric
  );
CREATE INDEX IF NOT EXISTS clothlog_tstzrange_idx ON clothlog USING GIST (timestamp);
create or replace
  function shiftdetect(stamp timestamp with time zone,
  out shiftname text,
  out shiftstart timestamp with time zone,
  out shiftend timestamp with time zone,
  out shiftdur interval)
   returns record
   language plpgsql
  as $function$
  declare
  dow numeric;

  weekday text;

  tz int;

  begin
  tz :=(extract(timezone_hour
  from
  stamp)::int);

  stamp :=(stamp at time zone 'utc' at time zone 'utc');

  dow := (
  select
    extract(ISODOW
  from
    stamp));

  weekday := (case
    when dow = 1 then 'monday'
    when dow = 2 then 'tuesday'
    when dow = 3 then 'wednesday'
    when dow = 4 then 'thursday'
    when dow = 5 then 'friday'
    when dow = 6 then 'saturday'
    when dow = 7 then 'sunday'
  end);

  execute 'select shiftname, make_timestamptz(extract(year from $1)::int,extract(month from $1)::int,extract(day from $1)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) as shiftstart, make_timestamptz(extract(year from $1)::int,extract(month from $1)::int,extract(day from $1)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration as shiftend from shiftconfig where ((' || weekday || ') and $1 >= make_timestamptz(extract(year from $1)::int,extract(month from $1)::int,extract(day from $1)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) and $1 < make_timestamptz(extract(year from $1)::int,extract(month from $1)::int,extract(day from $1)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration)'
  into
    shiftname,
    shiftstart,
    shiftend
      using stamp,
    'UTC';

  if (shiftname = '') is not false then

       execute 'select shiftname, make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) as shiftstart, make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration as shiftend from shiftconfig where ((' || weekday || ') and ((extract(hour from starttime at time zone $2)::int + $4)/ 24 ) > 0 and $1 >= make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) and $1 < make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration)'
  into
    shiftname,
    shiftstart,
    shiftend
      using stamp,
    'UTC',
    stamp - interval '1D',
    tz
               ;

  if (shiftname = '') is not false then

       weekday := (case
    when dow = 1 then 'sunday'
    when dow = 2 then 'monday'
    when dow = 3 then 'tuesday'
    when dow = 4 then 'wednesday'
    when dow = 5 then 'thursday'
    when dow = 6 then 'friday'
    when dow = 7 then 'saturday'
  end);

  execute 'select shiftname, make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) as shiftstart, make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration as shiftend from shiftconfig where ((' || weekday || ') and ((extract(hour from starttime at time zone $2)::int + $4)/ 24 ) = 0 and $1 >= make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2) and $1 < make_timestamptz(extract(year from $3)::int,extract(month from $3)::int,extract(day from $3)::int,extract(hour from starttime at time zone $2)::int,extract(minute from starttime at time zone $2)::int,0.0,$2)+duration)'
  into
    shiftname,
    shiftstart,
    shiftend
      using stamp,
    'UTC',
    stamp - interval '1D',
    tz
               ;
  end if;
  end if;

  shiftdur := shiftend - shiftstart;
  end;

  $function$
  ;

  drop trigger if exists linkDown
  on
tags;

drop function if exists modeCodeUpdate;

create or replace
function modeCodeUpdate()
 returns void
 language plpgsql
as $function$
declare
last_updated timestamptz;

new_updated timestamptz;

planSpeed numeric;

picksLastRun numeric;

realPicksLastRun numeric;

planDensity numeric;

warpShrinkage numeric;

orderLength numeric;

code numeric;

numcode numeric;

clock interval;

begin
select
	updated, val
into
	last_updated, numcode
from
	tags
where
	tag->>'name' = 'modeCode'
	and link = false;

if last_updated is not null
and last_updated < current_timestamp - interval '5 second'
and numcode <> 0 then

with numbers as (
select
	tag->>'name' as name,
	case
		when tag->>'name' = 'planSpeedMainDrive' then val
	end as val1,
	case
		when tag->>'name' = 'planClothDensity' then val
	end as val2,
	case
		when tag->>'name' = 'picksLastRun' then val
	end as val3,
	case
		when tag->>'name' = 'realPicksLastRun' then val
	end as val4,
	case
		when tag->>'name' = 'warpShrinkage' then val
	end as val5,
	case
		when tag->>'name' = 'orderLength' then val
	end as val6
from
	tags
where
	tag->>'name' in ('planSpeedMainDrive', 'planClothDensity', 'picksLastRun', 'realPicksLastRun', 'warpShrinkage', 'orderLength'))
      select
	into
	planSpeed,
	planDensity,
	picksLastRun,
	realPicksLastRun,
	warpShrinkage,
	orderLength
    (
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6)
	from
		numbers
	where
		name = 'planSpeedMainDrive'),
	(
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6)
	from
		numbers
	where
		name = 'planClothDensity'),
	(
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6,
		0)
	from
		numbers
	where
		name = 'picksLastRun'),
	(
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6,
		0)
	from
		numbers
	where
		name = 'realPicksLastRun'),
	(
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6)
	from
		numbers
	where
		name = 'warpShrinkage'),
	(
	select
		coalesce(val1,
		val2,
		val3,
		val4,
		val5,
		val6)
	from
		numbers
	where
		name = 'orderLength');

delete
from
	modelog
where
	upper_inf(timestamp)
	and current_timestamp<lower(timestamp);

delete
from
	clothlog
where
	upper_inf(timestamp)
	and current_timestamp<lower(timestamp);

update
	modelog
set
	timestamp = tstzrange(
      lower(timestamp),
	(case when last_updated > lower(timestamp) then last_updated else lower(timestamp) + interval '1 microsecond' end),
	'[)'
  ),
	picks = picksLastRun,
	realpicks = realPicksLastRun
where
	upper_inf(timestamp) returning modecode,
	((case when last_updated > lower(timestamp) then last_updated else lower(timestamp) + interval '1 microsecond' end) - lower(timestamp)),
  (case when last_updated > lower(timestamp) then last_updated else lower(timestamp) + interval '1 microsecond' end)
into
	code,
	clock,
  new_updated;

update
	tags
set
	val = val - (picksLastRun / (100 * planDensity * (1 - 0.01 * warpShrinkage))),
	updated = last_updated
where
	tag->>'name' = 'warpBeamLength';

update
	lifetime
set
	picks = picks + picksLastRun,
	cloth = cloth + (picksLastRun / (100 * planDensity)),
	motor = justify_hours(motor + case
		when code = 1 then clock
		else '0'
	end)
where
	serialno is not null;

insert
	into
	modelog
values(tstzrange(new_updated,
null,
'[)'),
0,
0,
planSpeed,
planDensity,
0
 );

if code = 6 then
update
	clothlog
set
	timestamp = tstzrange(
      lower(timestamp),
	last_updated,
	'[)'
  ),
	meters = orderLength
where
	event = 1
	and upper_inf(timestamp);

insert
	into
	clothlog
values(tstzrange(last_updated,
null,
'[)'),
1,
null);
end if;

UPDATE tags SET val=0 where tag->>'name'='modeCode' AND link=false;

end if;
end;

$function$
;

DROP TRIGGER IF EXISTS modeChanged
  ON tags;
DROP FUNCTION IF EXISTS modelog;
create or replace
function modelog()
 returns trigger
 language plpgsql
as $function$
declare
planSpeed numeric;

picksLastRun numeric;

realPicksLastRun numeric;

planDensity numeric;

warpShrinkage numeric;

orderLength numeric;

code numeric;

numcode numeric;

clock interval;

begin
if new.val is not null and new.link = true and old.link = false then
select
	modecode
into
	numcode
from
	modelog
where
  upper_inf(timestamp);
if numcode = 0 and new.val <> 0 then
update
	modelog
set
	timestamp = tstzrange(
      lower(timestamp),
	current_timestamp(3),
	'[)'
  ),
	picks = 0,
	realpicks = 0
where
	upper_inf(timestamp);
insert
	into
	modelog
values(tstzrange(current_timestamp(3),
null,
'[)'),
new.val,
null,
(select val from tags where tag->>'name' = 'planSpeedMainDrive'),
(select val from tags where tag->>'name' = 'planClothDensity'),
null
 );
end if;
end if;
if new.val is not null and new.link = true and old.link = true then
with numbers as (
  select
    tag->>'name' as name,
    case
      when tag->>'name' = 'planSpeedMainDrive' then val
    end as val1,
    case
      when tag->>'name' = 'planClothDensity' then val
    end as val2,
    case
      when tag->>'name' = 'picksLastRun' then val
    end as val3,
    case
      when tag->>'name' = 'realPicksLastRun' then val
    end as val4,
    case
      when tag->>'name' = 'warpShrinkage' then val
    end as val5,
    case
      when tag->>'name' = 'orderLength' then val
    end as val6
  from
    tags
  where
    tag->>'name' in ('planSpeedMainDrive', 'planClothDensity', 'picksLastRun', 'realPicksLastRun', 'warpShrinkage', 'orderLength'))
      select
    into
    planSpeed,
    planDensity,
    picksLastRun,
    realPicksLastRun,
    warpShrinkage,
    orderLength
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'planSpeedMainDrive'),
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'planClothDensity'),
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'picksLastRun'),
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'realPicksLastRun'),
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'warpShrinkage'),
    (
    select
      coalesce(val1,
      val2,
      val3,
      val4,
      val5,
      val6)
    from
      numbers
    where
      name = 'orderLength');
delete
from
	modelog
where
	upper_inf(timestamp)
	and current_timestamp<lower(timestamp);

delete
from
	clothlog
where
	upper_inf(timestamp)
	and current_timestamp<lower(timestamp);
if planSpeed is not null and planDensity is not null and picksLastRun is not null and realPicksLastRun is not null then
update
	modelog
set
	timestamp = tstzrange(
      lower(timestamp),
	current_timestamp(3),
	'[)'
  ),
	picks = picksLastRun,
	realpicks = realPicksLastRun
where
	upper_inf(timestamp) returning modecode,
	(current_timestamp(3)-lower(timestamp))
into
	code,
	clock;

update
	tags
set
	val = val - (picksLastRun / (100 * planDensity * (1 - 0.01 * warpShrinkage))),
	updated = current_timestamp
where
	tag->>'name' = 'warpBeamLength';

update
	lifetime
set
	picks = picks + picksLastRun,
	cloth = cloth + (picksLastRun / (100 * planDensity)),
	motor = justify_hours(motor + case when code = 1 then clock else '0' end)
where
	serialno is not null;

insert
	into
	modelog
values(tstzrange(current_timestamp(3),
null,
'[)'),
new.val,
null,
planSpeed,
planDensity,
null
 );
end if;

if new.val = 6 then
update
	clothlog
set
	timestamp = tstzrange(
      lower(timestamp),
	current_timestamp(3),
	'[)'
  ),
	meters = orderLength
where
	event = 1
	and upper_inf(timestamp);

insert
	into
	clothlog
values(tstzrange(current_timestamp(3),
null,
'[)'),
1,
null);
end if;
end if;
return null;
end;

$function$
;
create trigger modeChanged after insert or update on tags for row when (new.tag->>'name'='modeCode') execute function modelog();
DROP TRIGGER IF EXISTS modeupdate
  ON modelog;
DROP FUNCTION IF EXISTS modeupdate;
create or replace
function getstatinfo(starttime timestamp with time zone,
endtime timestamp with time zone)
 returns table(picks numeric,
meters numeric,
rpm numeric,
mph numeric,
efficiency numeric,
starts numeric,
runtime interval,
stops jsonb)
 language plpgsql
as $function$
declare
exdurs numeric;

begin
if starttime < current_timestamp then
	exdurs :=(
	with query as (
select
	modecode,
	upper(ts * tstzrange(starttime, endtime, '[)')) - lower(ts * tstzrange(starttime, endtime, '[)')) as dur
from
	modelog,
	lateral(
	select
		case
			when upper_inf(timestamp)
			and current_timestamp>lower(timestamp) then
            tstzrange(lower(timestamp),
			current_timestamp,
			'[)')
			else
            timestamp
		end as ts
	) newtimestamp
where
	tstzrange(starttime,
	endtime,
	'[)') && timestamp
)
select
	coalesce((extract(epoch
	from
		exdur)), 0) as exdurs
from
	lateral(
	select
		sum(dur) as exdur
	from
		query
	where
		modecode in (0, 2, 6)) normstop);

return QUERY (
with aquery as (
select
	ts as timestamp,
	modecode,
	plandensity,
	planspeed,
	modelog.picks,
  modelog.realpicks,
	up - low as dur
from
	modelog,
	lateral(
	select
		case
			when upper_inf(timestamp)
				and current_timestamp>lower(timestamp) then
            tstzrange(lower(timestamp),
				current_timestamp,
				'[)')
				else
            timestamp
			end as ts
	) newtimestamp,
	lateral (
	select
		lower(ts * tstzrange(starttime, endtime, '[)')) as low) low,
	lateral (
	select
		upper(ts * tstzrange(starttime, endtime, '[)')) as up) up
where
	tstzrange(starttime,
	endtime,
	'[)') && timestamp
),
bigquery as (
select
	sum(ppicks) as spicks,
	justify_hours(sum(dur)) as mototime,
	count(*) as runstarts,
	sum(
ppicks * 6000 /(planspeed * (durqs-exdurs))
 ) as eff,
	sum(
ppicks /(100 * plandensity)
) as meter
from
	aquery,
	lateral (
	select
		extract(epoch
	from
		(upper(timestamp)-lower(timestamp))) as durrs) rowsecduration,
	lateral (
	select
		extract(epoch
	from
		dur) as durs) intsecduration,
	lateral (
	select
  case
  when upper(timestamp)=current_timestamp
    and current_timestamp>lower(timestamp) then
(durs / durrs) * (
    select
      val
    from
      tags
    where
      (tag->>'name' = 'picksLastRun'))
    else
(durs / durrs) * aquery.picks
  end
	 as ppicks) partialpicks,
	lateral (
	select
		extract(epoch
	from
		(
		select
			sum(dur)
		from
			aquery)) as durqs) querysecduration
where
	modecode = 1
),
fquery as (
  select
    sum(fppicks) as fpicks,
    sum(rppicks) as rpicks,
    sum(
  fppicks /(100 * plandensity)
  ) as fmeter
  from
    aquery,
    lateral (
    select
      extract(epoch
    from
      (upper(timestamp)-lower(timestamp))) as fdurrs) frowsecduration,
    lateral (
    select
      extract(epoch
    from
      dur) as fdurs) fintsecduration,
    lateral (
    select
    case
    when upper(timestamp)=current_timestamp
      and current_timestamp>lower(timestamp) then
  (fdurs / fdurrs) * (
      select
        val
      from
        tags
      where
        (tag->>'name' = 'picksLastRun'))
      else
  (fdurs / fdurrs) * aquery.picks
    end
     as fppicks) fpartialpicks,
     lateral (
      select
      case
      when upper(timestamp)=current_timestamp
        and current_timestamp>lower(timestamp) then
    (fdurs / fdurrs) * (
        select
          val
        from
          tags
        where
          (tag->>'name' = 'realPicksLastRun'))
        else
    (fdurs / fdurrs) * aquery.realpicks
      end
       as rppicks) rpartialpicks
  )
select
	round(fquery.rpicks::numeric),
	fquery.fmeter::numeric,
	speedMainDrive::numeric,
	speedCloth::numeric,
	bigquery.eff::numeric,
	bigquery.runstarts::numeric,
	bigquery.mototime,
	descrstop
from
	bigquery,
  fquery,
	lateral(
	select
		round((bigquery.spicks * 60)/(
        select
          extract(epoch
        from
          bigquery.mototime) )) as speedMainDrive
    ) speedMainDrive,
	lateral(
	select
		bigquery.meter /(
		select
			extract(epoch
		from
			bigquery.mototime)/ 3600 ) as speedCloth) speedCloth,
	lateral (
      with t(num,
	stop) as (
	select
		*
	from
		(
	values (2,
	'button'),
	(6,
	'fabric'),
	(5,
	'tool'),
	(4,
	'weft'),
	(3,
	'warp'),
	(0,
	'other') ) as t(num,
		stop) )
	select
		jsonb_agg(json_build_object(t.stop, json_build_object('total', total , 'dur', justify_hours(dur)))) as descrstop
	from
		t,
		lateral(
		select
			count(*) as total,
			sum(aquery.dur) as dur
		from
			aquery
		where
			modecode = t.num) stat) descrstops
);
end if;
end;

$function$
;
CREATE OR REPLACE FUNCTION monthreport(stime timestamp with time zone, etime timestamp with time zone)
 RETURNS TABLE(starttime timestamp with time zone, endtime timestamp with time zone, picks numeric, meters numeric, rpm numeric, mph numeric, efficiency numeric, starts numeric, runtime interval, stops jsonb)
 LANGUAGE plpgsql
AS $function$
begin
return QUERY (
with dates as (
select
	date as st,
	date + interval '24 hours' as et
from
	generate_series(
        stime,
        etime,
        '1 day'
    ) date
)
select
	st,
	et,
	data.picks,
  data.meters,
  data.rpm,
  data.mph,
  data.efficiency,
  data.starts,
  data.runtime,
  data.stops
from
	dates,
  lateral(select * from getstatinfo(st,
    et) limit 1 ) data
);
end;

$function$
;
create or replace
function userreport(userid numeric,
stime timestamp with time zone,
etime timestamp with time zone)
 returns table(starttime timestamp with time zone,
endtime timestamp with time zone,
picks numeric,
meters numeric,
rpm numeric,
mph numeric,
efficiency numeric,
starts numeric,
runtime interval,
stops jsonb)
 language plpgsql
as $function$
begin
return QUERY (
with dates as (
select
	lower(tstzrange(stime, etime, '[)') * tr) as st,
	upper(tstzrange(stime, etime, '[)') * tr) as et
from
	userlog,
	lateral (
	select
		case
			when upper_inf(userlog.timestamp)
			and current_timestamp>lower(userlog.timestamp) then
              tstzrange(lower(userlog.timestamp),
			current_timestamp,
			'[)')
			else
              userlog.timestamp
		end as tr) timerange
where
	id = userid
	and role = 'weaver'
	and tstzrange(stime,
	etime,
	'[)') && tr
    ),
query as (
select
	ts as timestamp,
	low,
	up,
	tstzrange(dates.st,
	dates.et ,
	'[)') as usertr,
	modecode,
	plandensity,
	planspeed,
	modelog.picks,
  modelog.realpicks,
	up - low as dur
from
	modelog,
	dates,
	lateral(
	select
		case
			when upper_inf(timestamp)
				and current_timestamp>lower(timestamp) then
            tstzrange(lower(timestamp),
				current_timestamp,
				'[)')
				else
            timestamp
			end as ts
	) newtimestamp,
	lateral (
	select
		lower(ts * tstzrange(dates.st, dates.et, '[)')) as low) low,
	lateral (
	select
		upper(ts * tstzrange(dates.st, dates.et, '[)')) as up) up
where
	tstzrange(dates.st,
	dates.et,
	'[)') && timestamp
),
bigquery as (
select
	ppicks as spicks,
  rppicks as rpicks,
	timestamp,
	usertr,
	modecode,
	plandensity,
	planspeed,
	query.picks,
  query.realpicks,
	dur,
	ppicks /(100 * plandensity) as meter,
	planspeed * extract(epoch
from
	dur)/ 60 as planpicks
from
	query,
	lateral (
	select
		extract(epoch
	from
		(upper(timestamp)-lower(timestamp))) as durrs) rowsecduration,
	lateral (
	select
		extract(epoch
	from
		dur) as durs) intsecduration,
	lateral (
	select
		case
			when upper(timestamp)= current_timestamp
				and current_timestamp>lower(timestamp) then
(durs / durrs) * (
				select
					val
				from
					tags
				where
					(tag->>'name' = 'picksLastRun'))
				else
(durs / durrs) * query.picks
			end
	 as ppicks) partialpicks,
   lateral (
    select
      case
        when upper(timestamp)= current_timestamp
          and current_timestamp>lower(timestamp) then
  (durs / durrs) * (
          select
            val
          from
            tags
          where
            (tag->>'name' = 'realPicksLastRun'))
          else
  (durs / durrs) * query.realpicks
        end
     as rppicks) rpartialpicks
),
sftable as (
select
	modecode,
	usertr,
	sum(planpicks) as planpicks,
	justify_hours(sum(dur)) as dur,
	sum(spicks) filter (
where
	modecode = 1) as picks,
	sum (meter) filter (
where
	modecode = 1) as meters,
	sum(spicks) as ffpicks,
  sum(rpicks) as rrpicks,
	sum(meter) as ffmeter,
	round((sum(spicks) filter (
where
	modecode = 1) * 60)/(extract(epoch
from
	sum(dur) filter (
where
	modecode = 1)))) as rpm,
	(sum(meter) filter (
where
	modecode = 1))/(extract(epoch
from
	sum(dur) filter (
where
	modecode = 1))/ 3600 ) as mph,
	count(distinct timestamp) as starts,
	justify_hours(sum(dur)) as runtime
from
	bigquery
group by
	usertr,
	modecode)
select
	distinct on
	(usertr)
lower(usertr) as starttime,
	upper(usertr) as endtime,
	round(rpicks),
	fmeter,
	sftable.rpm,
	sftable.mph,
	sftable.picks * 100 / ppicks as efficiency,
	case
		when sftable.picks is null then 0
		else sftable.starts::numeric
	end,
	case
		when sftable.picks is null then null
		else sftable.runtime
	end,
	descrstop as stops
from
	sftable,
	lateral (
	select
		usertr as crow) crow,
	lateral(
	select
		sum(planpicks) as ppicks,
		sum(ffpicks) as fpicks,
    sum(rrpicks) as rpicks,
		sum(ffmeter) as fmeter
	from
		sftable
	where
		usertr = crow) ppicks,
	lateral (
	with t(num,
	stop) as (
	select
		*
	from
		(
	values (2,
	'button'),
	(6,
	'fabric'),
	(5,
	'tool'),
	(4,
	'weft'),
	(3,
	'warp'),
	(0,
	'other') ) as t(num,
		stop) )
	select
		jsonb_agg(json_build_object(t.stop,
		json_build_object('total',
		coalesce (total,
		0) ,
		'dur',
		dur))) as descrstop
	from
		t
	left join lateral(
		select
			sftable.starts as total,
			sftable.runtime as dur
		from
			sftable
		where
			usertr = crow
			and modecode = t.num) stat on
		true) descrstops
order by
	usertr,
	case
		modecode when 1 then 1
		else 2
	end
);
end;

$function$
;
create or replace
function getuserstatinfo(userid numeric,
starttime timestamp with time zone,
endtime timestamp with time zone)
 returns table(workdur interval,
picks numeric,
meters numeric,
rpm numeric,
mph numeric,
efficiency numeric,
starts numeric,
runtime interval,
stops jsonb)
 language plpgsql
as $function$
begin
return QUERY (
with dates as (
select
	lower(tstzrange(starttime, endtime, '[)') * tr) as st,
	upper(tstzrange(starttime, endtime, '[)') * tr) as et
from
	userlog,
	lateral (
	select
		case
			when upper_inf(userlog.timestamp)
			and current_timestamp>lower(userlog.timestamp) then
              tstzrange(lower(userlog.timestamp),
			current_timestamp,
			'[)')
			else
              userlog.timestamp
		end as tr) timerange
where
	id = userid
	and role = 'weaver'
	and tstzrange(starttime,
	endtime,
	'[)') && tr
    ),
query as (
select
	ts as timestamp,
	low,
	up,
	tstzrange(dates.st,
	dates.et ,
	'[)') as usertr,
	modecode,
	plandensity,
	planspeed,
	modelog.picks,
  modelog.realpicks,
	up - low as dur
from
	modelog,
	dates,
	lateral(
	select
		case
			when upper_inf(timestamp)
				and current_timestamp>lower(timestamp) then
            tstzrange(lower(timestamp),
				current_timestamp,
				'[)')
				else
            timestamp
			end as ts
	) newtimestamp,
	lateral (
	select
		lower(ts * tstzrange(dates.st, dates.et, '[)')) as low) low,
	lateral (
	select
		upper(ts * tstzrange(dates.st, dates.et, '[)')) as up) up
where
	tstzrange(dates.st,
	dates.et,
	'[)') && timestamp
),
bigquery as (
select
	ppicks as spicks,
  rppicks as rpicks,
	timestamp,
	usertr,
	modecode,
	plandensity,
	planspeed,
	query.picks,
  query.realpicks,
	dur,
	ppicks /(100 * plandensity) as meter,
	planspeed * extract(epoch
from
	dur)/ 60 as planpicks
from
	query,
	lateral (
	select
		extract(epoch
	from
		(upper(timestamp)-lower(timestamp))) as durrs) rowsecduration,
	lateral (
	select
		extract(epoch
	from
		dur) as durs) intsecduration,
	lateral (
	select
		case
			when upper(timestamp)= current_timestamp
				and current_timestamp>lower(timestamp) then
(durs / durrs) * (
				select
					val
				from
					tags
				where
					(tag->>'name' = 'picksLastRun'))
				else
(durs / durrs) * query.picks
			end
	 as ppicks) partialpicks,
   lateral (
    select
      case
        when upper(timestamp)= current_timestamp
          and current_timestamp>lower(timestamp) then
  (durs / durrs) * (
          select
            val
          from
            tags
          where
            (tag->>'name' = 'realPicksLastRun'))
          else
  (durs / durrs) * query.realpicks
        end
     as rppicks) rpartialpicks
),
sftable as (
select
	modecode,
	sum(planpicks) as planpicks,
	justify_hours(sum(dur)) as dur,
	sum(spicks) filter (
where
	modecode = 1) as picks,
	sum (meter) filter (
where
	modecode = 1) as meters,
	sum(spicks) as ffpicks,
  sum(rpicks) as rrpicks,
	sum(meter) as ffmeters,
	round((sum(spicks) filter (
	where modecode = 1) * 60)/(extract(epoch
from
	sum(dur) filter (
	where modecode = 1)))) as rpm,
	(sum(meter) filter (
where
	modecode = 1))/(extract(epoch
from
	sum(dur) filter (
where
	modecode = 1))/ 3600 ) as mph,
	count(distinct timestamp) as starts,
	justify_hours(sum(dur)) as runtime
from
	bigquery
group by
	modecode)
select
	workdurs,
	round(rpicks),
	fmeter,
	sftable.rpm,
	sftable.mph,
	sftable.picks * 100 / ppicks as efficiency,
	case
		when sftable.picks is null then 0
		else sftable.starts::numeric
	end,
	case
		when sftable.picks is null then null
		else sftable.runtime
	end,
	descrstop as stops
from
	sftable,
	lateral (
	select
		sum(sftable.ffpicks) as fpicks,
    sum(sftable.rrpicks) as rpicks,
		sum(sftable.ffmeters) as fmeter
	from
		sftable
	) fpicksmeter,
	lateral(
	select
		justify_hours(sum(et-st)) as workdurs
	from
		dates
	) workdurs,
	lateral(
	select
		sum(planpicks) as ppicks
	from
		sftable
	) ppicks,
	lateral (
	 with t(num,
	stop) as (
	select
		*
	from
		(
	values (2,
	'button'),
	(6,
	'fabric'),
	(5,
	'tool'),
	(4,
	'weft'),
	(3,
	'warp'),
	(0,
	'other') ) as t(num,
		stop) )
	select
		jsonb_agg(json_build_object(t.stop,
		json_build_object('total',
		coalesce (total,
		0) ,
		'dur',
		dur))
	order by
		case
			t.num when 2 then 1
			when 6 then 2
			when 5 then 3
			when 4 then 4
			when 4 then 5
			when 3 then 6
			else 7
		end) as descrstop
	from
		t
	left join lateral(
		select
			sftable.starts as total,
			sftable.runtime as dur
		from
			sftable
		where
			modecode = t.num) stat on
		true) descrstops
order by
	case
		modecode when 1 then 1
		else 2
	end
limit 1
);
end;

$function$
;
create or replace
function usersreport( starttime timestamp with time zone,
endtime timestamp with time zone)
 returns table(userid integer,
workdur interval,
picks numeric,
meters numeric,
rpm numeric,
mph numeric,
efficiency numeric,
starts numeric,
runtime interval,
stops jsonb)
 language plpgsql
as $function$
begin
return QUERY (
with t(userid) as (
select
	id as userid
from
	users
where
	role = 'weaver'
order by
	name
)
select
	*
from
	t,
	lateral(select * from getuserstatinfo(t.userid,
    starttime,
    endtime) limit 1 ) data);
end;

$function$
;
create or replace
function shiftsreport(stime timestamp with time zone,
etime timestamp with time zone)
 returns table(shiftname text,
starttime timestamp with time zone,
endtime timestamp with time zone,
picks numeric,
meters numeric,
rpm numeric,
mph numeric,
efficiency numeric,
starts numeric,
runtime interval,
stops jsonb)
 language plpgsql
as $function$
begin
return QUERY (
with shifts as (
with dates as (
select
	date as st,
	date + interval '24 hours' as et
from
	generate_series(
        stime,
        etime,
        '1 day'
    ) date
)
select
	shiftconfig.shiftname,
	(dates.st::date + shiftconfig.starttime) as start ,
	((dates.st::date + shiftconfig.starttime) + shiftconfig.duration ) as
end
from
dates,
shiftconfig,
lateral (
select
	extract(ISODOW
from
	st) as dow ) dow
where
(monday
	and dow = 1)
or (tuesday
	and dow = 2)
or(wednesday
	and dow = 3)
or(thursday
	and dow = 4)
or(friday
	and dow = 5)
or(saturday
	and dow = 6)
or(sunday
	and dow = 7)
  )
select
	shifts.shiftname,
	shifts.start,
	shifts.end,
	data.picks,
	data.meters,
	data.rpm,
	data.mph,
	data.efficiency,
	data.starts,
	data.runtime,
	data.stops
from
	shifts,
	lateral(select * from getstatinfo(shifts.start,
  shifts.end) limit 1 ) data
);
end;

$function$
;
CREATE TABLE IF NOT EXISTS reminders (
  id serial PRIMARY KEY,
  active boolean,
  title text,
  descr text,
  type smallint default 0,
  starttime timestamp with time zone default current_timestamp,
  runcondition numeric default 0.0,
  nexttime timestamp with time zone,
  nextrun numeric,
  acknowledged boolean default false
);
DROP TRIGGER IF EXISTS remupdate
  ON reminders;
DROP FUNCTION IF EXISTS reminders;
create or replace function reminders()
 returns trigger
 language plpgsql
as $function$
declare
  picksLastRun numeric;
  density numeric;
  clock interval;
begin
picksLastRun :=(
  select
    val
  from
    tags
  where
    (tag->>'name' = 'picksLastRun'));
density :=(
  select
    val
  from
    tags
  where
    (tag->>'name' = 'planClothDensity'));
clock :=(
  select
    (current_timestamp(3)-lower(timestamp))
  from
    modelog
  where
    modecode=1 and upper_inf(timestamp)
    limit 1);
if (TG_OP = 'UPDATE') then
  if old.type = 0 and (current_timestamp > (old.starttime + (interval '1' hour * old.runcondition))) then
    new.starttime := current_timestamp;
	  new.nexttime := current_timestamp + (interval '1' hour * new.runcondition);
    new.acknowledged := false;
  end if;
  if old.type = 1 and (((SELECT cloth from lifetime) + (picksLastRun / (100 * density))) > old.nextrun) then
    new.starttime := current_timestamp;
	  new.nextrun := new.runcondition*((((SELECT cloth from lifetime) + (picksLastRun / (100 * density)))/new.runcondition)+1);
    new.acknowledged := false;
  end if;
  if old.type = 2 and (extract(epoch from ((SELECT motor from lifetime) + coalesce(clock,'0'))) > old.nextrun) then
    new.starttime := current_timestamp;
	  new.nextrun := extract(epoch from ((SELECT motor from lifetime) + coalesce(clock,'0'))) + 3600 * new.runcondition;
    new.acknowledged := false;
  end if;
end if;

if (TG_OP = 'INSERT') then
	if (new.type=0) then
  		new.nexttime := new.starttime + (interval '1' hour * new.runcondition);
	end if;
	if (new.type=1) then
  		new.nextrun := new.runcondition*((((SELECT cloth from lifetime) + (picksLastRun / (100 * density)))/new.runcondition)+1);
	end if;
	if (new.type=2) then
  		new.nextrun := extract(epoch from ((SELECT motor from lifetime) + coalesce(clock,'0'))) + 3600 * new.runcondition;
	end if;
end if;
return new;
end;

$function$
;
create trigger remupdate before insert or update on reminders for row execute function reminders();
CREATE OR REPLACE FUNCTION getactualnotifications()
 RETURNS SETOF reminders
 LANGUAGE plpgsql
AS $function$
declare
  picksLastRun numeric;
  density numeric;
  clock interval;
begin
picksLastRun :=(
  select
    val
  from
    tags
  where
    (tag->>'name' = 'picksLastRun'));
density :=(
  select
    val
  from
    tags
  where
    (tag->>'name' = 'planClothDensity'));
clock :=(
  select
    (current_timestamp(3)-lower(timestamp))
  from
    modelog
  where
    modecode=1 and upper_inf(timestamp)
    limit 1);
  UPDATE reminders SET acknowledged=acknowledged;
  RETURN QUERY(SELECT * FROM reminders where active=true and acknowledged=false and current_timestamp >= starttime and
	case
  when type=0 then current_timestamp <= nexttime
  when type=1 then ((SELECT cloth from lifetime) + (picksLastRun / (100 * density))) <= nextrun
  when type=2 then extract(epoch from ((SELECT motor from lifetime) + coalesce(clock,'0'))) <= nextrun
  end
  );
end;

$function$
;
CREATE OR REPLACE FUNCTION getcurrentinfo(OUT tags jsonb, OUT rolls numeric, OUT shift jsonb, OUT lifetime jsonb, OUT weaver jsonb, OUT userinfo jsonb, OUT shiftinfo jsonb, OUT dayinfo jsonb, OUT monthinfo jsonb)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
  begin

  tags :=(
select
	jsonb_agg(e)
from
	(
	select
		tag,
		(round(val::numeric,(tag->>'dec')::integer)) as val,
		updated,
		link
	from
		tags
	where
		tag->>'group' = 'monitoring'
		or link = false) e);

rolls :=(
select
	count(*)
from
	clothlog
where
	not upper_inf(timestamp)
		and timestamp && tstzrange(lower((select timestamp from clothlog where upper_inf(timestamp) and event = 0)),
		current_timestamp(3),
		'[)')
			and event = 1);

shift :=(select row_to_json(e)::jsonb from (select * from shiftdetect(current_timestamp)) e);

lifetime :=(select row_to_json(e)::jsonb from (select type, serialno, mfgdate, round(picks) as picks, cloth, motor from lifetime) e);

weaver :=(
select
	row_to_json(e)::jsonb
from
	(
	select
		id,
		name,
		case when lower(timestamp) < current_timestamp then lower(timestamp) else current_timestamp end as logintime
	from
		userlog
	where
		upper_inf(timestamp)
		and role = 'weaver') e);

userinfo :=(
select
	row_to_json(e)::jsonb
from
	(
	select
		*
	from
		getuserstatinfo((weaver->>'id')::numeric,
		(weaver->>'logintime')::timestamptz,
		current_timestamp)) e);

if (shift->>'shiftname' = '') is not false then
  	shiftinfo := null;
else
  	shiftinfo :=(select row_to_json(e)::jsonb|| json_build_object('start', (shift->>'shiftstart')::timestamptz , 'end', current_timestamp)::jsonb  from (select * from getstatinfo((shift->>'shiftstart')::timestamptz, current_timestamp)) e);
end if;

dayinfo :=(
select
	row_to_json(e)::jsonb || json_build_object('start', date_trunc('day', current_timestamp) , 'end', current_timestamp)::jsonb
from
	(
	select
		*
	from
		getstatinfo(date_trunc('day', current_timestamp),
		current_timestamp)) e );

monthinfo := (select row_to_json(e)::jsonb || json_build_object('start', date_trunc('month', current_timestamp) , 'end', current_timestamp)::jsonb  from (select * from getstatinfo(date_trunc('month', current_timestamp), current_timestamp)) e);

end;

$function$
;
CREATE OR REPLACE FUNCTION getpartialinfo(OUT tags jsonb, OUT shift jsonb, OUT weaver jsonb, OUT userinfo jsonb, OUT shiftinfo jsonb, OUT dayinfo jsonb, OUT monthinfo jsonb)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
  begin

  tags :=(
select
	jsonb_agg(e)
from
	(
	select
		tag,
		(round(val::numeric,(tag->>'dec')::integer)) as val,
		updated,
		link
	from
		tags
	where
		tag->>'group' = 'monitoring'
		or link = false) e);

shift :=(select row_to_json(e)::jsonb from (select * from shiftdetect(current_timestamp)) e);

weaver :=(
select
	row_to_json(e)::jsonb
from
	(
	select
		id,
		name,
		case when lower(timestamp) < current_timestamp then lower(timestamp) else current_timestamp end as logintime
	from
		userlog
	where
		upper_inf(timestamp)
		and role = 'weaver') e);

userinfo :=(
select
	row_to_json(e)::jsonb
from
	(
	select
		picks, meters, rpm, mph, efficiency
	from
		getuserstatinfo((weaver->>'id')::numeric,
		(weaver->>'logintime')::timestamptz,
		current_timestamp)) e);

if (shift->>'shiftname' = '') is not false then
  	shiftinfo := null;
else
  	shiftinfo :=(select row_to_json(e)::jsonb|| json_build_object('start', (shift->>'shiftstart')::timestamptz , 'end', current_timestamp)::jsonb  from (select picks, meters, rpm, mph, efficiency from getstatinfo((shift->>'shiftstart')::timestamptz, current_timestamp)) e);
end if;

dayinfo :=(
select
	row_to_json(e)::jsonb || json_build_object('start', date_trunc('day', current_timestamp) , 'end', current_timestamp)::jsonb
from
	(
	select
		picks, meters, rpm, mph, efficiency
	from
		getstatinfo(date_trunc('day', current_timestamp),
		current_timestamp)) e );

monthinfo := (select row_to_json(e)::jsonb || json_build_object('start', date_trunc('month', current_timestamp) , 'end', current_timestamp)::jsonb  from (select picks, meters, rpm, mph, efficiency from getstatinfo(date_trunc('month', current_timestamp), current_timestamp)) e);

end;

$function$
;
`
export default createTableText
