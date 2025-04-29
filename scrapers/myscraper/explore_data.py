

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
    from pathlib import Path
    import ast
    from collections import Counter
    import json
    import re
    from tables import Dataset, engine
    from sqlalchemy.orm import sessionmaker 
    return (
        Counter,
        Dataset,
        Path,
        ast,
        engine,
        json,
        np,
        pd,
        plt,
        re,
        sessionmaker,
        sns,
    )


@app.cell
def _(Path):
    FIG_DIR = Path("fig")
    FIG_DIR.mkdir(exist_ok=True)
    return (FIG_DIR,)


@app.cell
def _(pd):
    dataset = pd.read_csv("out2.csv")
    dataset
    return (dataset,)


@app.cell
def _(dataset):
    no_title = dataset[dataset["title"].isnull()]
    no_title
    return


@app.cell
def _(dataset):
    dataset_non_null_title = dataset[~dataset["title"].isnull()]
    dataset_non_null_title
    return (dataset_non_null_title,)


@app.cell
def _(dataset_non_null_title):
    dataset_non_null_title[dataset_non_null_title["date_created"].isnull()]
    dataset_no_null_title_date= dataset_non_null_title[~dataset_non_null_title["date_created"].isnull()]
    dataset_no_null_title_date
    return (dataset_no_null_title_date,)


@app.cell
def _(dataset_no_null_title_date):
    dataset_no_null_title_date[
        (dataset_no_null_title_date["organization_type"].isnull()) &
        (dataset_no_null_title_date["publisher_heading"] != "District of Columbia")
    ]

    return


@app.cell
def _(dataset_no_null_title_date):
    dataset_no_null_title_date.loc[:, "organization_type"] = dataset_no_null_title_date["organization_type"].fillna("Federal District")
    dataset_no_null_title_date
    return


@app.cell
def _(dataset_no_null_title_date):
    dataset_no_null_title_date["organization_type"].unique()
    return


@app.cell
def _(dataset_no_null_title_date, plt):
    org_type_counts = dataset_no_null_title_date["organization_type"].value_counts()

    plt.figure(figsize=(8,6))
    org_type_counts.plot(kind="bar")
    plt.title("Number of Datasets by Organization Type")
    plt.xlabel("Organization Type")
    plt.ylabel("Dataset Count")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _(FIG_DIR, np, pd, plt, sns):
    def spiral_pack_bubbles(
        df,
        canvas_width=1600,
        canvas_height=1200,
        title="Spiral Packed Bubbles",
        label_if_count_ge=None,
        base_radius = 100, 
        circle_padding = 0.3,
        savefig=False,
    ):

        df = df.sort_values("count", ascending=False).reset_index(drop=True)
        max_count = df["count"].max()
        df["r"] = np.sqrt(df["count"]/max_count) * base_radius
        canvas_margin = 50

        def circles_overlap(x1, y1, r1, x2, y2, r2):
            return np.hypot(x1 - x2, y1 - y2) < (r1 + r2 + circle_padding)

        def is_valid_position(x, y, r, placed):
            if not (r + canvas_margin < x < canvas_width - r - canvas_margin):
                return False
            if not (r + canvas_margin < y < canvas_height - r - canvas_margin):
                return False
            for p in placed:
                if circles_overlap(x, y, r, p["x"], p["y"], p["r"]):
                    return False
            return True

        cx, cy = canvas_width / 2, canvas_height / 2
        placed = []

        for i, row in df.iterrows():
            r = row["r"]
            if i == 0:
                x, y = cx, cy
            else:
                found = False
                angle = 0
                spiral_radius = 0
                angle_step = np.radians(5)
                radius_growth = 0.3
                for _ in range(2000):
                    x = cx + spiral_radius * np.cos(angle)
                    y = cy + spiral_radius * np.sin(angle)
                    if is_valid_position(x, y, r, placed):
                        found = True
                        break
                    angle += angle_step
                    spiral_radius += radius_growth
                if not found:
                    x, y = -1000, -1000

            placed.append({
                "x": x,
                "y": y,
                "r": r,
                "label": row["publisher_heading"],
                "org": row["organization_type"],
                "count": row["count"]
            })

        packed_df = pd.DataFrame(placed)

        org_types = sorted(packed_df["org"].unique())
        palette = sns.color_palette("Set2", len(org_types))
        type_to_color = {org: palette[i] for i, org in enumerate(org_types)}
        colors = packed_df["org"].map(type_to_color)

        plt.figure(figsize=(20, 14))
        plt.scatter(
            packed_df["x"],
            packed_df["y"],
            s=(packed_df["r"] * 2)**2,
            c=colors,
            edgecolors="w",
            alpha=0.7
        )

        for _, row in packed_df.iterrows():
            if row["x"] > 0 and (label_if_count_ge is None or row["count"] >= label_if_count_ge):
                plt.text(
                    row["x"], row["y"],
                    f'{row["label"]}\n({row["count"]})',
                    ha='center', va='center',
                    fontsize=8, color='black'
                )

        for org, color in type_to_color.items():
            plt.scatter([], [], c=[color], label=org, alpha=0.6, s=100)

        plt.legend(title="Organization Type", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.title(title)
        plt.axis('off')
        plt.xlim(0, canvas_width)
        plt.ylim(0, canvas_height)
        plt.tight_layout()

        if savefig:
            save_path = FIG_DIR / f"{title.replace(' ', '_')}.png"
            plt.savefig(save_path, dpi=300)
            print(f"Saved figure to {save_path}")

        plt.show()

    return (spiral_pack_bubbles,)


@app.cell
def _(dataset_no_null_title_date, spiral_pack_bubbles):
    bubble_df = dataset_no_null_title_date.groupby(["publisher_heading", "organization_type"]) \
        .size().reset_index(name="count")
    bubble_df = bubble_df.sort_values("count", ascending=False).reset_index(drop=True)

    # Now run the function on full dataset, filtered <50, >50, and no "Federal"
    df_full = bubble_df.copy()
    df_over_50 = bubble_df[bubble_df["count"] > 50].copy()
    df_under_50 = bubble_df[bubble_df["count"] <= 50].copy()
    df_no_federal = bubble_df[bubble_df["organization_type"] != "Federal"].copy()

    spiral_pack_bubbles(df_full, title="All Publishers", label_if_count_ge=100, base_radius = 200, circle_padding=5,savefig=True)
    spiral_pack_bubbles(df_over_50, title="Publishers with Count > 50", base_radius =200, circle_padding=5,savefig=True)
    spiral_pack_bubbles(df_under_50, title="Publishers with Count ≤ 50", circle_padding=5,savefig=True)
    spiral_pack_bubbles(df_no_federal, title="All Except Federal Publishers", circle_padding=5,savefig=True)
    return


@app.cell
def _(dataset_no_null_title_date):
    empty_format_rows = dataset_no_null_title_date[
        dataset_no_null_title_date["normalized_formats"].apply(lambda x: x == [''])
    ]

    empty_format_rows
    return


@app.cell
def _():
    canonical_map = {
        "csv": {"csv", "csv file", "comma-separated values", "comma-separated values (.csv)",
                "csv file encoded with utf-8", "comma separated value", "comma separated vale","comma-separated-value", "Comma-Separated Values (CSV) text"},
        "json": {"json", "application/vnd.api+json", "zipped json", "project open data schema 1.1 json", "jsonld"},
        "xml": {"xml", "application/opensearchdescription+xml", "pdml", "application/simple-filter+xml"},
        "excel": {"excel", "microsoft excel", "xlsx", "xlsb", "microsoft excel file", "excel spreadsheet",
                  "excel workbook", "excel (xlsx)", "microsoft office - ooxml - spreadsheet (.xlsx)",
                  "microsoft excel worksheet", "microsoft excel document", "excel version of data tables for annual supplement to handbook 135 - 2024", "Excel spreadsheet with 4 tabs", "A Microsoft Excel spreadsheet", "Microsoft Excel File (.xlsx)"},
        "pdf": {"pdf", "pdf/a", "portable document format (pdf)", "pdf document", "a .pdf manuscript"},
        "html": {"html", "ht ml", "html web page", "application/html", "webpage", "web page with data links",
                 "html#data-access-examples", "html#windpower"},
        "zip": {"zip", "zipped", "zip format file", "zip file format", "compressed zip file containing csv data files",
                "zipped csv", "zipped accdb", "zipped binary text", "zip folder includes: fastq files (for 16s), r markdowns, rdata, csv, xlsx",
                "zip file contains fastq files within two folders depending on the donor", "compressed folder of code (.zip)",
                "zipped sas7bdat", "zipped sd2", "zipped sav", "zipped sys", "zipped por", "zipped dta",
                "zip file of iucr crystallographic information file (cif) datasets", "compressed folder with subfolders for each result in the paper and a readme",
                "zip file with html and jpeg formatted images", "zip file containing .dat files with single columns of values",
                "zip file containing .mat file for matlab", "a zip file containing .dat files with 5 columns",
                "this is a zipped complication of six .csv files and one .txt readme file.",
                "compressed ascii text file", "zip file containing mpeg-4 thermal videos",
                "zip file with .jpg and matlab .fig plots.", "zip file with .img raw ir camera files", "A zip file"},
        "text": {"text", "ascii", "plain text", "ascii text", "fixed-format text", "text/tab", "plain text file", 
                 "ascii numbers", "ascii text, two columns", "ascii text, columns", "plain, readable text",
                 "source code in plain text", "ascii text with crlf line terminators","application/txt",
                 "all data are given as .txt. files", "ascii text in comma separated variable (csv) format"},
        "image": {"jpeg", "jpg", "png", "gif", "image/jpg", "image/x-png", "jpeg formatted images", "rgb image"},
        "api": {"api", "api endpoint", "rest", "rest service", "esri rest", "arcgis geoservices rest api"},
        "geojson": {"geojson", "application/vnd.geo+json"},
        "tiff": {"tiff", "8 bit tiff image", "8bit tiff image", "tiff file format", ".zip archive containing tiff images"},
        "github": {"github repository", "github python files", "github repository for uuid", "github source repository", "github folder", "github repository url"},
        "python": {"python source code", "python scripts and jupyter notebooks", "python code", "python 3.8 module", "py"},
        "fortran": {"fortran 90 code, ascii", "fortran 77 program listing, ascii", "fortran 77 program ascii text",
                    "fortran 77 program, ascii", "fortran 90 code, mpi, ascii text"},
        "markdown": {"markdown", "text/markdown", "plain text / markdown"},
        "matlab": {
            "matlab code and app designer files", "matlab function", "matlab script", "matlab data file",
            "mat", "application/x-matlab-data", "matlab figure file", "matlab fig file", "application/mathematica",
            "matlab mat file", "zip file containing .mat file for matlab", "Matlab code and app designer files.", "mlx"
        }
    }


    # Reverse map
    alias_to_canonical = {}
    for canon, aliases in canonical_map.items():
        for alias in aliases:
            alias_to_canonical[alias.strip().lower()] = canon
    return (alias_to_canonical,)


@app.cell
def _(alias_to_canonical, ast, dataset_no_null_title_date):
    def parse_formats(cell):
        if isinstance(cell, list):
            return cell
        if isinstance(cell, str):
            # Try parsing list-like strings first
            try:
                parsed = ast.literal_eval(cell)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass
            # If comma-separated string, split
            if "," in cell:
                return [part.strip() for part in cell.split(",")]
            # Otherwise just return as a single-item list
            return [cell.strip()]
        return []

    # Apply it
    dataset_no_null_title_date["formats"] = dataset_no_null_title_date["formats"].apply(parse_formats)

    # Step 1: Convert stringified lists to real lists
    dataset_no_null_title_date["formats"] = dataset_no_null_title_date["formats"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )

    dataset_no_null_title_date["normalized_formats"] = dataset_no_null_title_date["formats"].apply(
        lambda format_list: [
            alias_to_canonical.get(fmt.strip().lower(), fmt)
            for fmt in format_list
            if fmt.strip() 
        ]
    )
    return


@app.cell
def _(dataset_no_null_title_date):
    dataset_no_null_title_date["normalized_formats"]
    return


@app.cell
def _(Counter, dataset_no_null_title_date, json, plt):
    all_formats = [
        tag
        for row in dataset_no_null_title_date["normalized_formats"].dropna()
        for tag in row if isinstance(tag, str)
    ]
    unique_formats = set(all_formats)
    format_counts = Counter(all_formats)
    # Filter counts ≥ 2
    filtered_counts = {fmt: count for fmt, count in format_counts.items() if count >= 20}
    sorted_format_counts = dict(sorted(format_counts.items(), key=lambda x: x[1], reverse=True))

    # Save to JSON
    json_path = "format_counts.json"
    with open(json_path, "w") as f:
        json.dump(sorted_format_counts, f, indent=2)

    # Plot
    filtered_formats, filtered_values = zip(*filtered_counts.items())

    plt.figure(figsize=(12, 7))
    plt.bar(filtered_formats, filtered_values)
    plt.xticks(rotation=45, ha='right', fontsize=8)
    plt.yticks(fontsize=8)
    plt.title("Formats Appearing at Least 20 Times", fontsize=12)
    plt.xlabel("Format", fontsize=10)
    plt.ylabel("Count", fontsize=10)
    plt.tight_layout()

    # Save figure
    fig_path = "format_counts.png"
    plt.savefig(fig_path, dpi=300)
    plt.show()
    return


@app.cell
def _(ast, dataset_no_null_title_date):
    def parse_tags(cell):
        if isinstance(cell, list):
            return cell
        if isinstance(cell, str):
            try:
                parsed = ast.literal_eval(cell)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass
            if "," in cell:
                return [tag.strip() for tag in cell.split(",")]
            return [cell.strip()]
        return []
    dataset_tags = dataset_no_null_title_date.copy()
    dataset_tags["tags_list"] = dataset_tags["tags"].apply(parse_tags)
    dataset_tags
    return (dataset_tags,)


@app.cell
def _(dataset_tags, re):
    def clean_tag(tag):
        tag = tag.lower().strip()
        tag = re.sub(r'\s*\/\s*', ' / ', tag)  # Normalize slashes
        tag = re.sub(r'\s*-\s*', ' ', tag)     # Break hyphens into spaces
        tag = re.sub(r'\s+', ' ', tag)          # Collapse multiple spaces
        return tag

    def clean_tag_list(tags):
        if not isinstance(tags, list):
            return []
        return [clean_tag(tag) for tag in tags if isinstance(tag, str) and tag.strip()]

    # Apply it
    dataset_tags["tags_normalized"] = dataset_tags["tags_list"].apply(clean_tag_list)
    dataset_tags

    return


@app.cell
def _(Counter, dataset_tags, json):
    all_tags = [
        tag
        for row in dataset_tags["tags_normalized"].dropna()
        for tag in row if isinstance(tag, str)]
    unique_tags = set(all_tags)
    tag_counts = Counter(all_tags)
    # Sort descending
    sorted_tag_counts = dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True))
    with open("tag_counts.json", "w") as tag_file:
        json.dump(sorted_tag_counts, tag_file, indent=2)
    unique_tags
    return


@app.cell
def _(dataset_tags, pd):
    df_final = dataset_tags.copy()
    # Use normalized_formats as formats
    df_final["formats_final"] = df_final["normalized_formats"]

    # Ensure 'formats_final' is always a list
    df_final["formats_final"] = df_final["formats_final"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # Clean tags to always be a list
    df_final["tags_final"] = df_final["tags_normalized"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # Fill text fields
    for col in ["title", "organization_type", "publisher_heading", "publisher"]:
        df_final[col] = df_final[col].where(
            df_final[col].notnull(), None
        )

    # Drop missing or duplicate URLs
    df_final.dropna(subset=["url"], inplace=True)
    df_final.drop_duplicates(subset=["url"], inplace=True)

    # Parse 'date_last_updated'
    if df_final["date_last_updated"].dtype == "object":
        df_final["date_last_updated"] = pd.to_datetime(
            df_final["date_last_updated"], errors="coerce"
        )

    return (df_final,)


@app.cell
def _(df_final):
    df_final
    return


@app.cell
def _(Dataset):
    def insert_datasets_batched(session, df, batch_size=1000):
        datasets = []
        for idx, (_, row) in enumerate(df.iterrows()):
            dataset = Dataset(
                title=row["title"],
                organization_type=row["organization_type"],
                formats=row["formats_final"],
                tags=row["tags_normalized"],
                publisher_heading=row["publisher_heading"],
                publisher=row["publisher"],
                date_last_updated=row["date_last_updated"],
                url=row["url"],
            )
            datasets.append(dataset)

            # Commit every batch_size
            if (idx + 1) % batch_size == 0:
                session.bulk_save_objects(datasets)
                session.commit()
                datasets.clear()
                print(f"Inserted {idx + 1} datasets...")

        # Insert any leftovers
        if datasets:
            session.bulk_save_objects(datasets)
            session.commit()
            print(f"Inserted {len(df)} total datasets.")

    return (insert_datasets_batched,)


@app.cell
def _(df_final, engine, insert_datasets_batched, sessionmaker):
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    insert_datasets_batched(session, df_final)
    return


if __name__ == "__main__":
    app.run()
