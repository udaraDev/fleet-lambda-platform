"""Generate the final EC8203 report PDF from verified project evidence."""
from pathlib import Path
from html import escape
from math import hypot
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    Table, TableStyle, KeepTogether, Flowable, Image)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'output' / 'pdf' / 'fleet-lambda-platform-report.pdf'

NAVY = colors.HexColor('#18354A')
TEAL = colors.HexColor('#167287')
PALE = colors.HexColor('#EAF1F4')
INK = colors.HexColor('#1C2730')
MUTED = colors.HexColor('#526570')


class Architecture(Flowable):
    def __init__(self):
        super().__init__(); self.width = 165*mm; self.height = 112*mm
    def draw(self):
        c = self.canv
        def box(x,y,w,h,title,sub):
            c.setFillColor(PALE); c.setStrokeColor(TEAL); c.roundRect(x,y,w,h,3*mm,fill=1)
            c.setFillColor(NAVY); c.setFont('Helvetica-Bold',9); c.drawCentredString(x+w/2,y+h-6*mm,title)
            c.setFillColor(INK); c.setFont('Helvetica',7.5)
            for i,line in enumerate(sub): c.drawCentredString(x+w/2,y+h-11*mm-i*4*mm,line)
        def arrow(x1,y1,x2,y2):
            c.setStrokeColor(MUTED); c.setFillColor(MUTED); c.line(x1,y1,x2,y2)
            length = hypot(x2-x1, y2-y1); ux,uy=(x2-x1)/length,(y2-y1)/length
            px,py=-uy,ux; bx,by=x2-ux*3*mm,y2-uy*3*mm
            c.line(x2,y2,bx+px*1.5*mm,by+py*1.5*mm)
            c.line(x2,y2,bx-px*1.5*mm,by-py*1.5*mm)
        box(0,82*mm,38*mm,24*mm,'Telemetry source',['Python, 12 vehicles','every 2 real seconds'])
        box(52*mm,82*mm,30*mm,24*mm,'Kafka',['trip-events','3 partitions'])
        box(96*mm,82*mm,43*mm,24*mm,'Spark speed',['live state + alerts','1-minute windows'])
        arrow(38*mm,94*mm,52*mm,94*mm); arrow(82*mm,94*mm,96*mm,94*mm)
        box(96*mm,45*mm,43*mm,24*mm,'Spark raw + MinIO',['Parquet by date','SHA-256 manifest'])
        box(0,45*mm,38*mm,24*mm,'Daily source',['expense CSV','one simulated day'])
        box(50*mm,45*mm,32*mm,24*mm,'Airflow',['profit + DQ DAGs','date isolation'])
        arrow(38*mm,57*mm,50*mm,57*mm); arrow(96*mm,57*mm,82*mm,57*mm)
        arrow(67*mm,82*mm,105*mm,69*mm)
        box(50*mm,8*mm,48*mm,24*mm,'Spark batch',['dedup + coverage','cost join + profit'])
        arrow(66*mm,45*mm,66*mm,32*mm)
        box(112*mm,8*mm,42*mm,24*mm,'PostgreSQL',['live + daily tables','versions + quarantine'])
        arrow(98*mm,20*mm,112*mm,20*mm)
        c.line(139*mm,94*mm,148*mm,94*mm); arrow(148*mm,94*mm,148*mm,32*mm)
        box(5*mm,8*mm,32*mm,24*mm,'Serving',['FastAPI + Prometheus','Grafana dashboards'])
        c.line(133*mm,8*mm,133*mm,3*mm); c.line(133*mm,3*mm,37*mm,3*mm)
        arrow(37*mm,3*mm,37*mm,8*mm)


def p(text, style='BodyX'):
    return Paragraph(text, styles[style])


def table(rows, widths=None, small=7.4):
    head = ParagraphStyle('TableHead', fontName='Helvetica-Bold', fontSize=small,
                          leading=small+2, textColor=colors.white)
    body = ParagraphStyle('TableBody', fontName='Helvetica', fontSize=small,
                          leading=small+2, textColor=INK)
    wrapped = [[Paragraph(escape(str(value)), head if row_index == 0 else body)
                for value in row] for row_index, row in enumerate(rows)]
    t = Table(wrapped, colWidths=widths, repeatRows=1, hAlign='LEFT')
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),NAVY),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTNAME',(0,1),(-1,-1),'Helvetica'),
        ('FONTSIZE',(0,0),(-1,-1),small),('LEADING',(0,0),(-1,-1),small+2),
        ('GRID',(0,0),(-1,-1),0.35,colors.HexColor('#9AABB4')),
        ('VALIGN',(0,0),(-1,-1),'TOP'),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,PALE]),
        ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
        ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    return t


def bullets(items):
    return [p('- '+x, 'BulletText') for x in items]


def header_footer(canvas, doc):
    canvas.saveState(); canvas.setStrokeColor(colors.HexColor('#B9C5CB'))
    canvas.line(20*mm,15*mm,190*mm,15*mm); canvas.setFont('Helvetica',7.5); canvas.setFillColor(MUTED)
    canvas.drawString(20*mm,10*mm,'EC8203 Applied Big Data Engineering | Fleet Lambda Platform')
    canvas.drawRightString(190*mm,10*mm,f'Page {doc.page}'); canvas.restoreState()


styles = getSampleStyleSheet()
styles.add(ParagraphStyle('TitleX', parent=styles['Title'], fontName='Helvetica-Bold',
    fontSize=27, leading=31, textColor=NAVY, alignment=TA_CENTER, spaceAfter=12))
styles.add(ParagraphStyle('SubTitle', parent=styles['Normal'], fontSize=12, leading=17,
    textColor=TEAL, alignment=TA_CENTER))
styles.add(ParagraphStyle('H1X', parent=styles['Heading1'], fontSize=20, leading=24,
    textColor=NAVY, spaceAfter=10))
styles.add(ParagraphStyle('H2X', parent=styles['Heading2'], fontSize=13, leading=16,
    textColor=TEAL, spaceBefore=10, spaceAfter=6))
styles.add(ParagraphStyle('BodyX', parent=styles['BodyText'], fontSize=9.2, leading=13,
    textColor=INK, spaceAfter=7))
styles.add(ParagraphStyle('BulletText', parent=styles['BodyText'], fontSize=9, leading=12.5,
    leftIndent=10, firstLineIndent=-7, textColor=INK, spaceAfter=3))
styles.add(ParagraphStyle('Callout', parent=styles['BodyText'], fontSize=9.2, leading=13,
    leftIndent=8, rightIndent=8, borderColor=TEAL, borderWidth=1, borderPadding=8,
    backColor=PALE, textColor=NAVY, spaceBefore=6, spaceAfter=10))
styles.add(ParagraphStyle('Small', parent=styles['BodyText'], fontSize=7.6, leading=10, textColor=MUTED))


story = []
story += [Spacer(1,35*mm), p('FLEET LAMBDA PLATFORM','TitleX'),
          p('Real-time ride-hailing operations and daily profitability reconciliation','SubTitle'),
          Spacer(1,18*mm), p('<b>EC8203 Applied Big Data Engineering Mini-Project</b>','SubTitle'),
          Spacer(1,22*mm), table([['Author','Programme'],['Udara Subodhitha Senevirathna','BSc Computer Engineering'],
          ['Institution','University of Ruhuna'],['Report date','23 September 2026']], [65*mm,80*mm],8.5),
          Spacer(1,16*mm), p('<b>Submission statement.</b> This report describes the delivered implementation and measured checks. It distinguishes local classroom evidence from production claims and does not claim a recorded demo video or production-scale capacity.','Callout'),
          PageBreak()]

story += [p('1. Use case and requirements','H1X'),
 p('A ride-hailing operator needs immediate visibility into fleet utilization and earnings while reconciling completed trips against daily fuel and maintenance costs. The two views have different truth requirements: live state may be indicative, while daily profit must not treat missing telemetry or missing costs as zero.'),
 p('Business questions','H2X')] + bullets([
 'What is current fleet utilization, idle ratio, trip activity and earnings by zone and time of day?',
 'Which vehicles are genuinely unprofitable after yesterday\'s costs, and which results remain unknown because evidence is incomplete?',
 'Can operators detect stalled ingestion, failed reconciliation, missing dates, damaged exports and bad source rows?']) + [
 p('Interpreted requirements','H2X'), table([
 ['ID','Requirement','Acceptance evidence'],
 ['R1','Continuous Python telemetry through Kafka partitions','Checkpointed Spark query and fresh-install smoke test'],
 ['R2','One expense file per simulated day','Producer creates dated CSV; Airflow discovers closed days'],
 ['R3','Meaningful cleaning, enrichment, aggregation and join','Native validation, time bucket, trip aggregate, coverage and cost join'],
 ['R4','Queryable serving and consolidated result','PostgreSQL, FastAPI, JSON report and business results page'],
 ['R5','Observable health and failures','Structured JSON logs; ingestion and report-integrity HTTP rules'],
 ['R6','Reproducible local execution','Compose, migrations, unit/integration tests and empty-volume verification']], [15*mm,70*mm,80*mm]),
 p('The persistent simulation clock starts at 2026-03-01 00:00 UTC. One simulated day equals 300 real seconds. A telemetry tick occurs every two real seconds and advances 9.6 simulated minutes. Times are UTC and monetary values are stored as integer LKR cents.','Callout'), PageBreak()]

story += [p('2. Architecture decision: Lambda vs Kappa','H1X'),
 p('Lambda was selected because corrected daily cost files must restate historical accounts while immutable trip evidence remains replayable. The speed path gives operational visibility; the batch path recomputes a vehicle-day result from committed archives and the latest valid expense file.'),
 table([['Decision factor','Lambda in this project','Kappa alternative'],
 ['Latency','Live state and lookbacks update every micro-batch.','A single stream could also meet low latency.'],
 ['Replay','Spark batch rereads verified Parquet by day.','Both expenses and telemetry must be durable versioned events.'],
 ['Consistency','Daily output is restated from a fixed input snapshot and marked by run/version.','Historical keyed state needs correction/retraction semantics.'],
 ['Cost/complexity','Two code paths but simple local recovery and explainable accounting.','One topology in principle, but more state/replay operational complexity here.'],
 ['Failure isolation','Live metrics can continue when one expense date fails.','A unified topology can couple operational and accounting failures.']], [31*mm,67*mm,67*mm]),
 p('Rejected alternative','H2X'), p('Kappa was rejected for this two-week local platform, not because Kappa is inferior in general. It becomes attractive when all sources are naturally versioned event streams and the team can operate replay, state migration and retractions. Daily CSV corrections make the separate batch truth path easier to verify and defend.'),
 p('Trade-off','H2X'), p('Lambda duplicates identity and transformation logic. This risk is mitigated by a shared fixed event-field contract, UTC timestamp canonicalization, parity tests, persistent identities, deterministic daily recomputation and explicit completeness states. Eventual consistency remains: a late correction is visible after the next Airflow restatement.'),
 p('<b>Consistency promise:</b> no end-to-end exactly-once claim. Kafka checkpoints, event identities, trip keys, archive manifests, database transactions and idempotent restatement provide practical replay safety within the documented single-writer dataset contract.','Callout'), PageBreak()]

story += [p('3. Delivered architecture and data flow','H1X'), Architecture(),
 p('Figure 1. Delivered local Lambda architecture. Kafka feeds independent speed and raw consumers; PostgreSQL serves FastAPI and Prometheus is visualised in Grafana. The MinIO archive, database and checkpoints are operated as one dataset.' ,'Small'),
 p('The speed query validates Kafka records, updates live state and computes one-minute event-time windows with a two-minute watermark. Per-event metric contributions allow the serving transaction to retract a discredited identity or trip and protect the corrected window from a later non-retracting Spark update. The raw query independently archives valid events to MinIO and publishes checksum manifests. A session advisory lock fences archive writes; a transaction lock protects serving commits. Airflow invokes Spark batch reconciliation independently for each ready date, newest first, with a configurable five-changed-date budget.'),
 PageBreak(), p('Storage contracts','H2X')] + bullets([
 'Raw MinIO Parquet files are accepted only through committed batch manifests containing row counts and SHA-256 digests.',
 'PostgreSQL stores latest vehicle state, completed trips, persistent event identities, conflicts, quarantine, run history and daily profitability.',
 'Daily JSON contains run ID, algorithm version, quality coverage and vehicle results. Its SHA-256 is stored in PostgreSQL and checked by health/retry logic.']) + [PageBreak()]

story += [p('4. Technology selection','H1X'), table([
 ['Component','Why selected','Constraint / rejected alternative'],
 ['Kafka 3.9','Required ingestion technology; keyed events and three partitions demonstrate partitioned transport.','Single broker is suitable only for a laptop; no replication.'],
 ['Spark 3.5 Structured Streaming','Required processing choice; native schema validation and checkpointed Kafka consumption.','Storm rejected to keep one engine across stream and batch.'],
 ['Spark DataFrames (batch)','Distributed trip aggregation, conflict detection, coverage and expense join.','Pure Python dictionaries were removed from production batch processing.'],
 ['Airflow 2.10','Scheduled discovery, retries, UI and serialized daily reconciliation.','One visible task is simpler but offers less stage-level retry detail.'],
 ['PostgreSQL 16','Transactions, constraints, JSON metadata and queryable serving.','Cassandra is less suited to relational reconciliation and local transactions.'],
 ['MinIO + Parquet','S3-compatible immutable replay source with typed timestamps, date partitions and verified manifests.','Single-node MinIO is for local reproducibility, not high availability.'],
 ['FastAPI','Typed validation, API documentation, health endpoints and a small business view.','Airflow UI is not a fleet dashboard.'],
 ['Docker Compose','Repeatable single-host wiring and persistent named volumes.','Not production orchestration.']], [28*mm,70*mm,67*mm],7.1),
 p('The implementation pins major Python dependencies and container images. Host tests use Python 3.11 where possible; CI installs the development requirements and validates Compose configuration. Local demo credentials and loopback-only ports are not suitable for external deployment.','Callout'), PageBreak()]

story += [p('5. Implementation','H1X'), p('Streaming path','H2X')] + bullets([
 'Python emits telemetry for twelve vehicles; every fourth vehicle remains parked to create an interpretable idle/cost-only case.',
 'Both consumers use from_json plus native column constraints. Unknown vehicles, malformed JSON, impossible coordinates/speed, invalid types and out-of-bounds simulated timestamps are rejected.',
 'Kafka is capped at 2,000 offsets per trigger and serving micro-batches at 2,500 rows. Accepted serving rows are collected only inside this explicit classroom bound, then written with bulk SQL operations.',
 'Persistent event fingerprints deduplicate across batches. Same identity with different business content becomes a conflict; implicated trip revenue and live state are removed rather than arbitrarily selected.']) + [
 p('Batch path','H2X')] + bullets([
 'Only ledger-committed and checksum-verified Parquet enters reconciliation. Spark detects event/trip conflicts, aggregates completions by vehicle and calculates telemetry coverage.',
 'All registered vehicles remain visible. Complete telemetry plus expenses yields profit; missing telemetry, conflicts or missing expenses yields a null profit with a quality status.',
 'A greater-than-five-percent expense rejection rate fails that date without replacing the last good result. Other dates continue and the Airflow task reports a failure summary.',
 'Database rows move to pending before file export. Atomic file replacement then publishes a digest. Missing/corrupt published files are automatically regenerated.']) + [
 p('Serving semantics','H2X'), p('The live fleet endpoint uses a latest-state simulated-time lookback. The zone endpoint accepts a 1-1440 minute window and reads Spark event-time rows, including conflict-retracted windows. Both are explicitly labelled indicative. The daily endpoint returns vehicle rows and publication metadata from one SQL statement, avoiding mismatched versions during concurrent publication. The browser page refreshes every 15 seconds and presents LKR values, report quality and algorithm version.'), PageBreak()]

story += [p('6. Observability and failure behaviour','H1X'), table([
 ['Signal','Detection','Response / meaning'],
 ['Live freshness','Accepted-event arrival age plus simulated event lag','HTTP 503 after 120 real seconds; duplicates cannot refresh health'],
 ['Batch failures','Latest run status per date and stalled-run threshold','HTTP 503; failed date retained with error'],
 ['Date completeness','Every expected date from simulation start to latest closed day','Missing historical hole makes report health degraded'],
 ['Publication','Pending state, algorithm version and JSON SHA-256','Missing/corrupt/outdated report makes health degraded and triggers restatement'],
 ['Data quality','Quarantine rows and >5% daily gate','Bad date fails; last good daily result remains'],
 ['Tracing','trace_id in source/live trip records and structured logs','Supports event walkthrough; not full distributed tracing'],
 ['Business alert','Observed idle_since against simulated latest event','API lists considerably idle vehicles']], [31*mm,67*mm,67*mm],7.2),
 p('Structured JSON logs include timestamp, stage and message plus batch/run identifiers, row counts and errors where relevant. The official Prometheus Python client exports metrics; a pinned Prometheus server evaluates three local rules and a provisioned Grafana dashboard visualises pipeline health. External notification routing is deliberately outside the local submission.'),
 p('Failure exercise','H2X'), p('Final verification stopped only the telemetry producer. Ingestion health returned HTTP 503 at a 126-second event age, then the cleanup handler restarted the producer and health recovered to HTTP 200 at 5.6 seconds. The demo runbook repeats this exercise. Because the simulation wall clock continues, an outage creates an honest historical coverage gap; the system must not fabricate events or profit.'),
 p('<b>Recovery rule:</b> PostgreSQL, Kafka, Parquet and Spark checkpoint form one logical dataset. Coordinated backups are required. Deleting one component alone is unsupported. Conflicts require audited source correction and a deliberate rebuild, not ad-hoc deletion.','Callout'), PageBreak()]

story += [p('7. Results and business output','H1X'),
 p('The running platform exposes a consolidated business page at http://localhost:8001. It combines current reporting/active vehicles, idle ratio, hourly earnings, zone activity and a selectable daily vehicle profitability table. It also states the report run, publication state, algorithm version and quality outcome.'),
 Image(str(ROOT / 'output' / 'evidence' / 'dashboard.png'), width=165*mm, height=105*mm),
 p('Figure 2. Actual local results page showing healthy ingestion, the live fleet summary and the parameterised Spark event-time zone view. The selectable daily table continues below the captured viewport.','Small'),
 p('Verified snapshots','H2X'), table([
 ['Evidence','Observed result'],
 ['Automated unit/API/archive tests','48 passed, 1 dependency-gated skip and 17 subtests; Spark parity also passed explicitly'],
 ['Isolated Spark/PostgreSQL suite','23 named checks passed; disposable schema and temporary files'],
 ['Fresh volumes','Sixteen-service Compose definition; DAG imports, report, live, MinIO and monitoring checks'],
 ['Short throughput smoke test','10/100/500 target eps: all 30/300/1,500 events accepted; p95 latency 8.48/6.03/6.15 s'],
 ['Final report-integrity health','Healthy; 280 dates; zero missing, invalid, outdated, failed, stalled or pending'],
 ['Final result quality snapshot','1,568 complete vehicle-days; 1,792 incomplete-telemetry vehicle-days']], [61*mm,104*mm]),
 p('The historical counts are a dated snapshot, not a performance benchmark. Incomplete days reflect real downtime in the persistent demonstration dataset. All retained dates were restated under algorithm version 4 so export digests and identity semantics were recalculated rather than inherited.','Callout'), PageBreak()]

story += [p('8. Verification and reproducibility','H1X'),
 p('The verification strategy separates pure logic, API behaviour, isolated database/Spark integration, live smoke checks and a fresh-volume installation. Faults that would damage evidence are applied only to temporary directories and UUID-named schemas.'),
 table([['Area','Checks'],
 ['Replay/integrity','Cross-batch duplicate, timestamp spelling parity, conflicting fare, committed archive loss, writer fence'],
 ['Financial truth','Trip counts, zero-revenue parked vehicle, corrected cost, missing expense, incomplete telemetry, profit equation'],
 ['Publication','Pending state, simulated replace failure, retry, missing JSON recovery, corrupt JSON recovery, digest/run match'],
 ['Validation','Malformed JSON, unknown vehicle, future timestamp and daily cost quality gate'],
 ['Concurrency','Same-date Airflow lock and consistent one-statement API read'],
 ['Operations','Compose schema, DAG imports, all endpoints, empty PostgreSQL/Kafka/archive/checkpoint volumes']], [38*mm,127*mm]),
 p('Fresh-install evidence','H2X'), p('An isolated Compose project used empty uniquely named volumes, alternate localhost ports and the already built images. It generated live data and a daily report, passed the read-only smoke test, showed no Airflow import errors, and was then stopped. Its disposable volumes were removed after machine-readable evidence was saved. This validates initialization on the same Docker host; it does not claim an uncached second-machine download test.'),
 p('Reproduction','H2X')] + bullets([
 'Copy .env.example to .env only when no real .env exists, then run docker compose up --build -d.',
 'Run python -m unittest discover -s tests -v and python -m scripts.verify_running --wait-seconds 480.',
 'Run the isolated reconciliation verification in the application image; it cleans only its UUID schema.',
 'Use scripts/verify_clean_install.py when its alternate ports are free; it records evidence, stops containers and removes only its uniquely named disposable volumes.']) + [PageBreak()]

story += [p('9. Limitations, scale and security','H1X'), table([
 ['Current limitation','Production direction'],
 ['Single Kafka broker / local[2] Spark','Replicated Kafka, distributed Spark and capacity testing'],
 ['Bounded driver collection and PostgreSQL merge','Partition staging and set-based final merge after measured load'],
 ['Small immutable MinIO Parquet files','Iceberg/Delta compaction, lifecycle policies and replicated object storage'],
 ['One Airflow task','Stage-specific tasks/sensors after operational need is demonstrated'],
 ['Two-minute watermark is a classroom policy','Tune allowed lateness from measured production arrival distributions'],
 ['Local credentials, no API auth/TLS','Secrets manager, least-privilege roles, authentication, TLS and network policy'],
 ['Local Prometheus/Grafana; no routed notifications','Production SLOs, Alertmanager routes and on-call ownership'],
 ['Manual conflict resolution','Audited correction/rebuild workflow and lineage tooling'],
 ['Short 500 eps smoke test only','Long-duration saturation, resource profiling and realistic arrival distributions']], [58*mm,107*mm]),
 p('The project implements MinIO, separate consumers, formal windows, Grafana and durable alert history. It still treats them as local teaching infrastructure: correctness, architecture reasoning and reproducible evidence remain more important than naming production tools.'),
 p('Security scope','H2X'), p('Host ports bind to loopback and credentials are documented as local-demo only. Before external deployment, rotate secrets, give FastAPI a read-only database role, separate migration/write roles, authenticate endpoints, enable TLS and define retention/privacy controls for location traces.'),
 p('Ethical interpretation','H2X'), p('Profitability is an operational vehicle measure, not a driver-performance score. Missing data and conflicts are shown as unknown to reduce harmful inference. A production system would require governance for location access, retention and human review of alerts.'), PageBreak()]

story += [p('10. Conclusion and references','H1X'),
 p('The delivered platform meets the core mini-project objective: two simulated sources feed an observable Lambda pipeline; independent Spark consumers provide raw and speed layers; Spark also performs historical reconciliation; Airflow orchestrates profitability and data-quality work; results land in queryable PostgreSQL and versioned exports; and FastAPI, Prometheus and Grafana expose operational and financial answers.'),
 p('The strongest engineering decision is not a particular tool but the treatment of uncertainty. Duplicate identities are idempotent, conflicts invalidate authority, missing telemetry does not become zero revenue, bad cost files preserve the last good result, and published files are checked rather than assumed. The result is a defensible teaching system with explicit boundaries.'),
 p('References','H2X')] + bullets([
 'EC8203 Data Engineering Mini-Project brief, 2026, pages 1-5.',
 'Apache Kafka 3.9 documentation: https://kafka.apache.org/39/',
 'Apache Spark 3.5.6 Structured Streaming programming guide and Kafka integration documentation: https://spark.apache.org/docs/3.5.6/',
 'Apache Airflow 2.10.5 documentation: https://airflow.apache.org/docs/apache-airflow/2.10.5/',
 'PostgreSQL 16 documentation: https://www.postgresql.org/docs/16/',
 'FastAPI documentation: https://fastapi.tiangolo.com/']) + [
 p('Submission artefacts','H2X'), table([['Artefact','Location / purpose'],
 ['Source repository/ZIP','Complete code, Compose, migrations, tests and CI workflow'],
 ['This PDF','Architecture decision, design, evidence, limitations and references'],
 ['docs/DEMO_RUNBOOK.md','Prepared approximately eight-minute live demonstration'],
 ['docs/VIVA_QA.md','Ten likely viva questions with honest prepared answers'],
 ['docs/CONTRIBUTION_STATEMENT.md','Individual authorship and contribution statement'],
 ['docs/FINAL_SCOPE.md','Authoritative delivered scope and deferrals'],
 ['output/evidence/clean-install.json','Machine-readable fresh-volume verification evidence'],
 ['output/evidence/performance-benchmark.json','10/100/500 eps latency and acceptance evidence']], [55*mm,110*mm]),
 p('Contribution note. The README identifies one author. No additional group-member contributions are invented. If submitted as group work, the students must add a truthful statement based on actual contributions.','Callout')]

OUT.parent.mkdir(parents=True, exist_ok=True)
doc = SimpleDocTemplate(str(OUT), pagesize=A4, rightMargin=20*mm, leftMargin=20*mm,
                        topMargin=20*mm, bottomMargin=22*mm,
                        title='Fleet Lambda Platform - EC8203 Mini-Project',
                        author='Udara Subodhitha Senevirathna')
doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
print(OUT)
