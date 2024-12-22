create or replace database arctic_qs;
create or replace schema hol;
use warehouse compute_wh;

use schema arctic_qs.hol;
CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage call_transcripts_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/misc/call_transcripts/';

CREATE or REPLACE table CALL_TRANSCRIPTS ( 
  date_created date,
  language varchar(60),
  country varchar(60),
  product varchar(60),
  category varchar(60),
  damage_type varchar(90),
  transcript varchar
) COMMENT = '{"origin":"sf_sit-is", "name":"aiml_notebooks_artic_cortex", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

COPY into CALL_TRANSCRIPTS
  from @call_transcripts_data_stage;


-- select metadata$filename, $1, $2, $3 from @call_transcripts_data_stage;

-- Translation
select snowflake.cortex.translate('wie geht es dir heute?','de_DE','en_XX');

select transcript,snowflake.cortex.translate(transcript,'de_DE','en_XX') from call_transcripts where lower(language) = 'german';

-- Sentiment Analysis
select transcript, snowflake.cortex.sentiment(transcript) from call_transcripts where lower(language) = 'english';

-- Summarization
select transcript,snowflake.cortex.summarize(transcript) as summary from call_transcripts where lower(language) = 'english' limit 1;

select transcript,snowflake.cortex.summarize(transcript) as summary,snowflake.cortex.count_tokens('summarize',transcript) as number_of_tokens from call_transcripts where lower(language) = 'english' limit 1;

-- Text Classification
select transcript,snowflake.cortex.classify_text(transcript,['Refund','Exchange']) as classification from call_transcripts where lower(language) = 'english';

-- Summarization using Prompt
SET prompt = 
'### 
Summarize this transcript in less than 200 words. 
Put the product name, defect and summary in JSON format. 
###';

select snowflake.cortex.complete('snowflake-arctic',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where lower(language) = 'english' limit 1;

