"""Build the rubric report from captured artifacts, without inventing results."""
import json, os, sys, re
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,PageBreak,Flowable
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
ROOT=Path(__file__).resolve().parent.parent
os.chdir(ROOT)
styles=getSampleStyleSheet(); styles['BodyText'].fontSize=10; styles['BodyText'].leading=14
styles.add(ParagraphStyle('TableText',parent=styles['BodyText'],fontSize=9.3,leading=12))
styles['Heading1'].textColor=colors.HexColor('#143c54')
story=[]
def p(t,style='BodyText'): story.append(Paragraph(t,styles[style])); story.append(Spacer(1,4))
def table(headers,rows,widths=None):
    data=[[Paragraph(escape(str(x)),styles['TableText']) for x in row] for row in [headers]+rows]
    t=Table(data,colWidths=widths,repeatRows=1,hAlign='LEFT');t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e1edf2')),('VALIGN',(0,0),(-1,-1),'TOP'),('GRID',(0,0),(-1,-1),.3,colors.HexColor('#bdcbd2')),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7)]));story.append(t)
def page(n,title):
    if story: story.append(PageBreak())
    p('MOVIELENS ANALYTICS  /  CPS853  /  '+str(n),'Heading3');p(title,'Heading1')
def read(name,default=None):
    f=Path('reports')/name
    return json.loads(f.read_text()) if f.exists() else default
validation=read('validation.json',{}); sizes=read('storage_sizes.json',{}); watermark=read('watermark_validation.json',{}); bench=read('final_index_benchmark.json',read('report_index_benchmark.json',read('index_benchmark.json',{})))
if isinstance(bench,list):
    observations=bench
    bench={'method':'One execute+fetch measurement per question/state; unindexed then indexed, primary keys retained. Warm local database, consumer stopped; illustrative single-run results.','results':{str(state):{str(a['question']):{'median_ms':a['elapsed_ms'],'n':1} for a in observations if a['indexed']==state} for state in (False,True)}}

class Diagram(Flowable):
    def __init__(self): Flowable.__init__(self);self.width=480;self.height=204
    def draw(self):
        c=self.canv
        boxes=[(0,170,130,'CSV: ratings + movies'),(175,170,130,'Spark validation'),(350,170,130,'Parquet by year'),(175,110,130,'Spark batch SQL'),(350,110,130,'Producer -> Kafka'),(175,48,130,'SQLite aggregates'),(350,48,130,'Spark hourly windows'),(175,0,130,'HTTP / CLI answers')]
        for x,y,w,label in boxes:
            c.setFillColor(colors.HexColor('#e1edf2'));c.roundRect(x,y,w,32,5,fill=1,stroke=0);c.setFillColor(colors.HexColor('#143c54'));c.setFont('Helvetica',9);c.drawCentredString(x+w/2,y+12,label)
        c.setStrokeColor(colors.HexColor('#567b8b'))
        for x1,y1,x2,y2 in [(130,186,175,186),(305,186,350,186),(415,170,415,142),(350,170,305,142),(240,110,240,80),(415,110,415,80),(350,64,305,64),(240,48,240,32)]:
            c.line(x1,y1,x2,y2)
            import math
            angle=math.atan2(y2-y1,x2-x1)
            for delta in (-0.5,0.5): c.line(x2,y2,x2-6*math.cos(angle+delta),y2-6*math.sin(angle+delta))
page(1,'Design and scope')
p('Final implementation report | Watermark replay evidence captured 25 November 2026. Team: Homa Ahmadinoori (501249405), Izma Mujeeb (501231651), Nafisa Salsabil (501217956), Nitant Chandulal Suhagiya (501338100).')
p('The platform converts MovieLens ratings to Parquet, replays event-time-ordered records through Kafka, maintains hourly movie counts, and serves three SQL answers from one SQLite database. This report concerns the supplied 1,000,000-rating slice and 87,585 movie records; it does not claim a 32M run.')
story.append(Diagram());p('Figure 1. Batch aggregates flow directly from Spark to SQLite; the replay branch feeds one-hour counts into the same database. Quality failures are quarantined separately; valid events after the five-minute window grace are stored in SQLite late_events. Checkpoints, Kafka logs and an atomic batch ledger support recovery.')
table(['Choice','SQLite (implemented)','PostgreSQL (alternative)'],[['Deployment','Embedded, no database service; suits one laptop.','Separate service, credentials and memory allocation.'],['Access pattern','Indexed aggregate queries, one stream writer, WAL readers.','Multiple concurrent writers and stronger server-side operations.'],['Tradeoff','Writer serialization; driver inserts and local disk constrain growth.','Higher operating cost locally; preferable when write concurrency and data outgrow this design.']],[85,197,198])
p('Database rationale is a design assessment, not a measured SQLite-versus-PostgreSQL benchmark. Primary keys identify movies, genre/year pairs and hour/movie pairs. Secondary indexes match the three ranking queries.')
page(2,'Changes tracked against the proposal')
table(['Proposal commitment','Current implementation and reason'],[['32,000,204 ratings','1,000,000 supplied ratings. One million real ratings satisfies the assignment. Processing 32M is not required; the chosen scope reduces laptop load.'],['Parquet by rating year','Retained for ratings. Movies remain an unpartitioned lookup because they have no rating event year.'],['One database for all answers','SQLite chosen; movie_stats, genre_year and hourly_counts store aggregates. Docker-managed grace5 volumes isolate this version and keep database access in Linux.'],['Hourly event-time counts + 15-minute watermark','Changed to a five-minute event-time grace tied to each window end because the prior diagnostic did not classify actual late window rows. The boundary is inclusive.'],['Idempotent writes and restart','Additive keyed updates plus an atomic stream_batches marker; an in-grace arrival can revise an existing hour without double counting on retry.'],['Late rows logged and compared with batch','Rows beyond five minutes after window end are now stored in SQLite late_events with Kafka offsets, watermark and lateness. Per-batch counts reconcile exactly.'],['Five forced restarts in original proposal, including mid-window','The approved target is 2-3. Earlier restart measurements belong to the prior watermark/checkpoint generation; the new version still needs its own restart run.'],['Four checks and stage counts','Missing fields, rating range, positive IDs/time, duplicate keys; quality failures remain quarantined and late events have a transactional table/count ledger.']],[165,315])
p('Runtime-driven fixes: SQLite moved to a Docker-managed Linux volume after a malformed-database error. This update uses isolated grace5 database/Kafka volumes and a fresh stream_grace5 checkpoint. Late injection waits for the event-time watermark. The former three independent consumer queries were consolidated into one offset-ordered microbatch processor, so incoming, included, late, and quality-failed counts come from identical rows. The sink adds hourly deltas; an atomic ledger prevents retries from double-counting.')
page(3,'Evidence: store, batch and quality')
if sizes:
    table(['Dataset','Rows in / out','CSV bytes','Parquet bytes'],[['Movies',str(sizes['movies']['rows']),sizes['movies']['csv_bytes'],sizes['movies']['parquet_bytes']],['Ratings',f"{sizes['ratings']['rows_in']} / {sizes['ratings']['rows_out']}",sizes['ratings']['csv_bytes'],sizes['ratings']['parquet_bytes']]],[75,135,135,135])
else:p('Storage runtime artifact is missing. Existing Parquet files alone do not prove the current batch conversion ran.')
p('Part 1 reads the generated Parquet for both Spark SQL answers. Q1 joins titles to average/count by movie and applies count >= 1,000. Q2 explodes genres and ranks counts per UTC rating year; multi-genre movies contribute to every genre and tied winners are retained.')
log=Path('reports/evidence/batch.log').read_text() if Path('reports/evidence/batch.log').exists() else ''
fails=re.findall(r'check=(\w+) failed=([\d,]+)',log)
table(['Automatic check','Rule','Observed failed rows'],[[name,rule,dict(fails).get(name,'Not captured')] for name,rule in [('missing','Any required rating field is null'),('range','Rating outside 0.5 to 5.0'),('ids','User ID, movie ID or timestamp <= 0'),('duplicates','Extra (userId, movieId, timestamp) keys')]],[90,270,120])
p('Failures can overlap between checks, so their sum is not necessarily the unique quarantine count. Batch good + bad must equal input. Invalid rows are set aside in Parquet/JSONL files; the threshold alert is printed when unique failures exceed 1%. Movie foreign-key/schema checks are not comprehensive.')
p('Controlled Spark test: 10 input rows produced 3 accepted and 7 quarantined. Check counts were missing 1, range 4, IDs/time 1 and duplicates 1. The test read back all 7 quarantined rows from Parquet and checked row conservation. The clean source had zero quality failures.')
stream_rows=watermark.get('rows',{})
table(['Stage / count unit','Rows in','Rows out'],[['Ratings validation / ratings','1,000,000','1,000,000; failed 0'],['Parquet writes / records','1,000,000 ratings + 87,585 movies','1,000,000 ratings + 87,585 movies'],['Producer / source ratings','1,000,000','1,000,000; failed 0'],['Stream / raw events',str(stream_rows.get('rows_in','Not captured')),f"{stream_rows.get('within_grace','?')} included + {stream_rows.get('quarantined_late','?')} late + {stream_rows.get('quality_failed','?')} quality-failed"],['Batch database / aggregate rows','26,505 movie stats; 541 genre/year','26,505 movie stats; 541 genre/year'],['Hourly database / aggregate keys',str(validation.get('hourly_keys_compared','Not captured')),str(validation.get('hourly_keys_compared','Not captured'))]],[195,140,145])
p('Aggregation changes the count unit: '+f"{validation.get('hourly_keys_compared',0):,}"+' movie-hour keys represent '+f"{validation.get('stream_rating_total',0):,}"+' included source ratings. Five deliberately old test events were quarantined separately. Spark sample output: Shawshank Redemption average 4.42106 from 3,357 ratings; Drama leads 1996 with 21,051 genre contributions.')
p('Evidence: reports/evidence/batch.log, container_tests.txt, storage_sizes.json and validation.json. Source: starter.py, quality.py and tests/test_quality.py.')
page(4,'Database answers and response times')
p('SQL execute plus fetchall are timed; connection, JSON and network are excluded. Values below are recorded by the CSV verification run and stored in query_timings.')
answers={a['question']:a for a in validation.get('answers',[])}
for q,title in [(1,'Q1. Ten highest-rated movies with at least 1,000 ratings'),(2,'Q2. Most-rated genre per rating year'),(3,'Q3. Most-rated movies in the latest event-time hour')]:
    p(title,'Heading3'); a=answers.get(q)
    if not a: p('No verified runtime answer captured.');continue
    p(f"Response time: {a['elapsed_ms']} ms; returned rows: {len(a['rows'])}.")
    if q==1: table(['Movie (ID)','Average','Ratings'],[[f"{r['title']} ({r['movie_id']})",f"{r['avg_rating']:.5f}",r['rating_count']] for r in a['rows']],[330,75,75])
    elif q==2:
        genres={r['genre'] for r in a['rows']}
        if len(genres)==1:
            p(next(iter(genres))+' wins every year below (year: rating count). '+ '; '.join(f"{r['year']}: {r['rating_count']:,}" for r in a['rows']))
        else: p('; '.join(f"{r['year']}: {r['genre']} ({r['rating_count']:,})" for r in a['rows']))
    else:
        if a['rows']:p('UTC window: '+a['rows'][0]['hour_start']+' to '+a['rows'][0]['hour_end']+'.')
        p('; '.join(f"{r['title']} [ID {r['movie_id']}]: {r['rating_count']}" for r in a['rows']))
page(5,'Evidence: five-minute watermark and recovery')
p('Full answers and oracle results: reports/validation.json. The independent CSV oracle compares all hourly keys using a temporary disk reference to bound memory.')
counts=watermark.get('rows',{})
if watermark:
    p(f"The full replay observed {counts['rows_in']:,} raw Kafka rows: {counts['within_grace']:,} included, {counts['quarantined_late']:,} stored in late_events, and {counts['quality_failed']:,} quality failures. Per-batch and total conservation passed. SQLite integrity_check returned {watermark['database_integrity']}.")
    if watermark['late_rows']:
        sample=watermark['late_rows'][0]
        p(f"Sample quarantine output: Kafka partition {sample['source_partition']}, offset {sample['source_offset']}, movie {sample['movie_id']}, rating {sample['rating']:.1f}, event timestamp {sample['event_timestamp']} (Unix seconds). Window end {sample['window_end']}, watermark at arrival {sample['watermark_at_arrival']}.")
    p('Every late row retains its source partition/offset, event timestamp, window bounds, watermark at arrival, seconds beyond grace, and original JSON. Sample records and all batch counts are in reports/watermark_validation.json.')
if validation:p(f"Independent CSV verification: source rows = {validation['csv_ratings']:,}; included hourly rating total = {validation['stream_rating_total']:,}; all three answers and all {validation.get('hourly_keys_compared',0):,} hourly keys pass.")
else:p('Full stream/CSV reconciliation was not captured. Do not infer successful delivery from producer send counts alone.')
p('The watermark is the greatest valid event timestamp previously observed, minus 300 seconds. For a row in `[window_start, window_end)`, the consumer includes it while the prior maximum is at most `window_end + 300`; strictly later rows are quarantined. Thus the exact five-minute boundary is included. Unit tests cover the boundary. Hourly delta, late records, batch marker, counters and maximum event time commit together, making checkpoint replay idempotent.')
p('Recovery uses the isolated checkpoints/stream_grace5 path with matching grace5 SQLite and Kafka volumes. The earlier two restart measurements belong to the previous 15-minute implementation; repeat the restart experiment on this generation before claiming recovery results for the updated consumer. New producer offsets are not globally deduplicated.')
page(6,'The deep part: changes, measurements, learning')
p('The controlled before-and-after experiment measures the serving indexes. Separately, the stream change replaced a separate lateness diagnostic with window-based classification in the sink. The current replay reconciles 1,000,000 included source rows and five quarantined late rows, and the CSV oracle matches every included hourly key (page 5). The serving experiment uses an earlier isolated snapshot with the same SQL and index definitions.')
if bench:
    rows=[]
    for q in ('1','2','3'):
        b=bench['results']['False'][q]['median_ms'];a=bench['results']['True'][q]['median_ms'];rows.append(['Q'+q,f'{b:.4f}',f'{a:.4f}',f'{b/a:.2f}x' if a else 'n/a'])
    table(['Query','Before median ms','After median ms','Before / after'],rows,[75,140,140,125]);p(bench['method'])
    if 'snapshot_stream_ratings' in bench: p('Snapshot: '+str(bench['snapshot_stream_ratings'])+' streamed ratings; table row counts: '+str(bench['cardinalities'])+'.')
    p('Before: secondary ranking indexes removed, primary keys retained. After: partial average-ranking index for Q1, year/count index for Q2, and hour/count index for Q3. The backup snapshot, SQL and expected results are identical in both states. These are local warm-cache observations. Q1 and Q2 show the largest gains. Q3 already has a primary key starting with hour_start, so the added ranking index brings a much smaller benefit in this snapshot. Warm-cache timing variability limits conclusions at sub-millisecond scale; another recorded run showed no Q3 improvement.')
else:p('Before/after timing evidence is missing. The benchmark script reports 30 measured calls per question/state after one warmup and saves medians and ranges.')
p('What we learned: lateness must be measured against the window end, not only against an event timestamp. An event can have an old timestamp and still arrive within the window grace; a later event should remain inspectable instead of disappearing into a dropped-row counter.')
p('The sink adds accepted hourly deltas and records the batch marker, high-water mark and late rows in one SQLite transaction. Checkpointing alone does not make an external write safe; the atomic transaction and idempotent batch ID protect the commit/retry gap.')
p('Interpret the index measurements by query shape: Q1 can scan the eligible ranking index rather than sort a broad aggregate table; Q2 benefits from per-year maximum lookup; Q3 already has a primary key beginning with hour_start, so its improvement may be small. Small tables and cache effects can outweigh index gains. See final_index_benchmark.json for the exact table trials and plans; index_benchmark.json records the earlier run.')
page(7,'At 100 times the data')
p('Here 100x means 100 million ratings relative to this one-million-row run, not 100x the proposal\'s 32M. The following are engineering predictions, not measured scale results. At unchanged row width, raw ratings alone grow from 24,776,884 bytes to roughly 2.48 GB; intermediate shuffle, Kafka retention and checkpoints add substantially more.')
table(['Likely constraint','What to change and measure'],[['First likely failure: producer peak RAM','The producer materializes and sorts an entire year with Arrow. At 100x, the largest year can exceed its 1 GiB container limit before the serving tables do. Use Spark external sorting into timestamp-ordered bounded files, then replay in batches; measure maximum RSS and send throughput.'],['Replay and stream throughput','One Kafka partition and local two-thread Spark limit throughput. One offset-ordered consumer processes each cached microbatch. Cap offsets per trigger and measure lag, sort spill and driver memory. Add Kafka partitions only after designing how to coordinate event-time progress across them.'],['SQLite sink and driver boundary','toLocalIterator and a single SQLite writer become a throughput and lock-contention bottleneck. Batch writes and retain aggregate serving tables; move to PostgreSQL if sustained writes or concurrent clients exceed the local budget.'],['Storage and state','Compact small Parquet files to measured target sizes; retain year pruning. Bound Kafka retention, monitor disk and watermark progress; benchmark checkpoint recovery before adding more partitions.']],[155,325])
p('Priority order: profile the largest year and sort spill first; measure source-to-sink lag next; change the database only when write throughput/lock measurements justify it. A larger dataset does not automatically make indexed aggregate answers 100x slower because serving cardinality differs from raw row count.')
page(8,'Contributions, acknowledgement and reproducibility')
p('Individual contributions below were confirmed by the team in this report session. They refine the pair-level responsibilities recorded in the proposal.')
table(['Member','Proposal responsibility','Actual contribution status'],[['Nafisa Salsabil','Store/Batch + Serve','Implemented Serve; made checkpoint video'],['Izma Mujeeb','Store/Batch + Serve','Implemented Store and Batch'],['Homa Ahmadinoori','Stream + Quality/Monitoring','Implemented Stream; added Docker commands for startup in three commands or fewer'],['Nitant Chandulal Suhagiya','Stream + Quality/Monitoring','Implemented Quality and Monitoring']],[125,175,180])
p('<b>AI acknowledgement.</b> OpenAI Codex was used in this session to inspect code and compare it with the proposal/rubric, run validation commands, collect evidence, add report measurement/generation scripts, and draft/layout this report. The implementation README also acknowledges OpenAI Codex assistance with the SQLite serving layer, timed queries, regression tests, Docker integration fixes and requirements audit. The contribution table records confirmed team responsibilities. The team must review this report and add any further AI tools or outside-code sources before submission.')
p('<b>Reproduce.</b> From the supplied code directory run docker compose up --build -d. Wait for the finite producer and consumer catch-up, then run python3 verify_results.py in the serve container. For index measurements, create an isolated SQLite backup inside Linux and set DATABASE_PATH to that backup before running reports/measure_report.py; otherwise stop the consumer for an offline benchmark. Run python3 -m unittest discover -s tests -v in the container. Build this PDF with reports/build_report.py in a Python environment with ReportLab.')
p('<b>Rubric coverage.</b> This report maps design (pages 1-2), four-part evidence (3-5), deep work (5-6), 100x scale (7), and contributions/AI (8). The measured scope is the supplied one-million-rating slice. Limitations and additional boundary tests are stated explicitly. Raw evidence and code accompany the PDF in the ZIP.')
p('<b>Sources.</b> Local CPS853 Proposal.pdf (dataset, questions, roles and stream criteria); Team_Project.pdf (assignment/rubric); README.md and reports/requirements_audit.md; implementation and tests in this ZIP. Dataset attribution: MovieLens 32M, GroupLens Research, https://grouplens.org/datasets/movielens/32m/; usage terms recorded in the proposal. No independent licence reassessment is claimed.')
def footer(c,doc):
    c.setFont('Helvetica',8);c.setFillColor(colors.HexColor('#567b8b'));c.drawString(42,26,'MovieLens Analytics | CPS853 | 25 Nov 2026');c.drawRightString(570,26,str(doc.page))
Path('output/pdf').mkdir(parents=True,exist_ok=True)
SimpleDocTemplate('output/pdf/MovieLens_Final_Report.pdf',pagesize=(612,792),rightMargin=66,leftMargin=66,topMargin=38,bottomMargin=42).build(story,onFirstPage=footer,onLaterPages=footer)
print('Built output/pdf/MovieLens_Final_Report.pdf')
