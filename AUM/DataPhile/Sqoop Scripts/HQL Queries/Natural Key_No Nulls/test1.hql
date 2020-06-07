select cast(regexp_replace('20200430','(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date);
