delimiter $$
CREATE FUNCTION DATE_TRUNC(field ENUM('microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', 'millennium'), source datetime(6))
RETURNS datetime(6)
DETERMINISTIC
BEGIN
  IF field IN ('millisecond') THEN SET source = source - INTERVAL MICROSECOND(source) % 1000 MICROSECOND; END IF;
  IF field NOT IN ('microsecond', 'millisecond') THEN SET source = source - INTERVAL MICROSECOND(source) MICROSECOND; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second') THEN SET source = source - INTERVAL SECOND(source) SECOND; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second', 'minute') THEN SET source = source - INTERVAL MINUTE(source) MINUTE; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second', 'minute', 'hour') THEN SET source = source - INTERVAL HOUR(source) HOUR; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second', 'minute', 'hour', 'day') THEN SET source = source - INTERVAL DAYOFWEEK(source) - 1 DAY; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week') THEN SET source = source - INTERVAL DAY(source) - 1 DAY; END IF;
  IF field IN ('quarter') THEN SET source = source - INTERVAL MONTH(source) % 3 - 1 MONTH; END IF;
  IF field NOT IN ('microsecond', 'millisecond', 'second', 'minute', 'hour', 'week', 'day', 'month', 'quarter') THEN SET source = source - INTERVAL MONTH(source) - 1 MONTH; END IF;

  -- Year ranges go from 1 - 10, e.g. 1961-1970, not 1960-1969. The third millenium started 2001, not 2000. If you want it the other way, remove the "- 1" from each of the following.
  IF field IN ('decade') THEN SET source = source - INTERVAL YEAR(source) % 10 - 1 YEAR; END IF;
  IF field IN ('century') THEN SET source = source - INTERVAL YEAR(source) % 100  - 1 YEAR; END IF;
  IF field IN ('millennium') THEN SET source = source - INTERVAL YEAR(source) % 1000 - 1 YEAR; END IF;
 
  RETURN source;
END