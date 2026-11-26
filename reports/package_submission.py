"""Package current source, real inputs, report and evidence without mutable runtime state."""
from pathlib import Path
import zipfile,hashlib,json
root=Path(__file__).resolve().parent.parent
paths=[]
for p in root.rglob('*'):
    if not p.is_file():continue
    rel=p.relative_to(root)
    if any(x in {'.git','__pycache__','.pytest_cache','tmp','output','data','checkpoints','late_state','late_ratings','bad_ratings','bad_ratings_stream','node_modules','movies.parquet','ratings.parquet','stream_hourly_counts','recording-runtime'} for x in rel.parts):continue
    if p.suffix in {'.pyc','.DS_Store','.zip','.mp4','.mov'}:continue
    if rel.parts[0].startswith('.') and str(rel) not in {'.gitignore','.dockerignore'}:continue
    paths.append((p,rel))
pdf=root/'output/pdf/MovieLens_Submission_Report.pdf'
source_pdf=root/'output/pdf/MovieLens_Final_Report.pdf'
assert source_pdf.is_file()
pdf.write_bytes(source_pdf.read_bytes())
paths.append((pdf,Path('MovieLens_Final_Report.pdf')))
payloads=[(rel,p.read_bytes()) for p,rel in paths]
manifest={str(rel):{'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()} for rel,data in payloads}
out=root/'output/MovieLens_Final_Submission.zip';out.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as z:
    for rel,data in payloads:z.writestr(str(Path('movie-reviews')/rel),data)
    z.writestr('movie-reviews/SUBMISSION_MANIFEST.json',json.dumps(manifest,indent=2))
with zipfile.ZipFile(out) as z:assert z.testzip() is None
print(out, out.stat().st_size, 'bytes;', len(paths), 'files')
