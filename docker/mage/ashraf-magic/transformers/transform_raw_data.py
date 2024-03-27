from typing import Dict, List
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    dfs = []
    for message in messages:
        # Convert bytes to string and load JSON
        print('Message',message)
        data_dict = message["data"]
        # data_dict = json.loads(data_string)

        # Create DataFrame for each message
        df = pd.DataFrame([data_dict])
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    result_df = pd.concat(dfs, ignore_index=True)

    # Specify your transformation logic here

    return result_df