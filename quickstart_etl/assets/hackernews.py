import base64
from io import BytesIO
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(group_name="hackernews", compute_kind="HackerNews API")
def hackernews_topstory_ids() -> List[int]:
    """Get up to 500 top stories from the HackerNews topstories endpoint.

    API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
    """
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_500_newstories = requests.get(newstories_url).json()
    return top_500_newstories


@asset(group_name="hackernews", compute_kind="HackerNews API")
def hackernews_topstories(
    context: AssetExecutionContext, hackernews_topstory_ids: List[int]
) -> pd.DataFrame:
    """Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.

    API Docs: https://github.com/HackerNews/API#items
    """
    results = []
    for item_id in hackernews_topstory_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
    # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
    # This metadata would be useful for monitoring and maintaining the asset as you iterate.
    # Read more about in asset metadata in https://docs.dagster.io/concepts/assets/software-defined-assets#recording-materialization-metadata
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df