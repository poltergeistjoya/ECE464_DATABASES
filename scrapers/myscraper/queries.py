from sqlalchemy import select, func, true, or_, literal, any_
from sqlalchemy.orm import sessionmaker, aliased
from tables import Dataset, engine
from collections import Counter
from typing import List

def search_datasets_by_tag(session, search_term, limit:int):
    search_term = search_term.lower()

    stmt = select(Dataset).where(
        literal(search_term).ilike(any_(Dataset.tags))
    ).limit(limit)

    return session.execute(stmt).scalars().all()

def search_datasets_by_partial_tag(session, search_term: str, limit: int = 10):
    pattern = f"%{search_term}%"
    
    # Unnest tags
    tag_alias = func.unnest(Dataset.tags).alias('tag')
    tags_subquery = (
        select(1)
        .where(tag_alias.column.ilike(pattern))
        .correlate(Dataset)
        .scalar_subquery()
    )
    
    stmt = (
        select(Dataset)
        .where(
            or_(
                Dataset.title.ilike(pattern),
                func.exists(tags_subquery)
            )
        )
        .limit(limit)
    )
    
    return session.execute(stmt).scalars().all()

def formats_used_less_than(session, threshold=10):
    stmt = select(func.unnest(Dataset.formats))
    results = session.execute(stmt).scalars().all()

    counts = Counter(results)
    filtered = {fmt: count for fmt, count in counts.items() if count < threshold}
    return dict(sorted(filtered.items(), key=lambda x: x[1]))

def get_random_dataset(session):
    #statement query 
    stmt = select(Dataset).order_by(func.random()).limit(1)
    #query execution
    return session.execute(stmt).scalar_one()

def get_all_unique_tags(session):
    stmt = (
        select(func.unnest(Dataset.tags).label("tag"), func.count().label("count"))
        .group_by("tag")
        .order_by(func.count().desc())
    )
    results = session.execute(stmt).all()
    return results  # list of (tag, count) tuples

def get_datasets_by_format(session, format_name: str, limit:int):
    format_name = format_name.lower()

    formats_subq = select(func.unnest(Dataset.formats).label("fmt")).lateral()
    formats_alias = aliased(formats_subq)

    stmt = (
        select(Dataset)
        .join(formats_alias, true())
        .where(func.lower(formats_alias.c.fmt).ilike(f"%{format_name}%")).limit(limit)
    )

    return session.execute(stmt).scalars().all()

def search_datasets(session, search_term: str, limit:int):
    pattern = f"%{search_term}%"
    
    tag_alias = func.unnest(Dataset.tags).alias('tag')

    tags_subquery = (
        select(1)
        .where(tag_alias.column.ilike(pattern))
        .correlate(Dataset)
        .scalar_subquery()
    )

    stmt = (
        select(Dataset)
        .where(
            or_(
                Dataset.title.ilike(pattern),
                func.exists(tags_subquery)
            )
        )
        .limit(limit)
    )

    return session.execute(stmt).scalars().all()

if __name__ == "__main__":
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    print("# Random dataset")
    random_dataset = get_random_dataset(session)
    print("| Title | URL |")
    print("|:------|:----|")
    print(f"| {random_dataset.title} | {random_dataset.url} |")

    print("\n# Formats used less than 10 times")
    print("| Format | Count |")
    print("|:-------|------:|")
    for fmt, count in formats_used_less_than(session, threshold=10).items():
        print(f"| {fmt} | {count} |")

    print("\n# Top 20 tags")
    print("| Tag | Count |")
    print("|:----|------:|")
    for tag, count in get_all_unique_tags(session)[:20]:
        print(f"| {tag} | {count} |")

    print("\n# 10 Datasets with 'geojson' format")
    print("| Title | URL |")
    print("|:------|:----|")
    for dataset in get_datasets_by_format(session, "geojson", limit=10):
        print(f"| {dataset.title} | {dataset.url} |")

    print("\n# 50 Datasets with 'horse' tag (exact match)")
    print("| Title | URL |")
    print("|:------|:----|")
    for dataset in search_datasets_by_tag(session, "horse", limit=50):
        print(f"| {dataset.title} | {dataset.url} |")

    print("\n# 50 Datasets with 'horse' partial match in tags")
    print("| Title | Matching Tag | URL |")
    print("|:------|:--------------|:----|")
    results = search_datasets_by_partial_tag(session, "horse", limit=50)
    for dataset in results:
        matching_tags = [tag for tag in dataset.tags if "horse" in tag.lower()]
        for tag in matching_tags:
            print(f"| {dataset.title} | {tag} | {dataset.url} |")

    print("\n# 50 Datasets with 'horse' partial match in title or tags")
    print("| Title | URL |")
    print("|:------|:----|")
    for dataset in search_datasets(session, "horse", limit=50):
        print(f"| {dataset.title} | {dataset.url} |")
