"""This script uses Plotly to visualize benchmark results.

To use this script run

```shell
.venv/bin/python ./scripts/plot_results.py
```
"""

import os
import copy

import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from utils import DEFAULT_PLOTS_DIR, TIMINGS_FILE, WRITE_PLOT

# colors for each bar
# COLORS = {
#     "xorbits": "#f7c5a0",
#     "dask": "#87f7cf",
#     "pandas": "#72ccff",
#     # "modin": "#d4a4eb",
# }

# default base template for plot's theme
DEFAULT_THEME = "plotly_dark"

# other configuration
BAR_TYPE = "group"
LABEL_UPDATES = {
    "x": "query",
    "y": "seconds",
    "color": "Solution",
    "pattern_shape": "Solution",
}


# def add_annotations(fig, limit: int, df: pd.DataFrame):
#     # order of solutions in the file
#     # e.g. ['polar', 'pandas', 'dask']
#     bar_order = (
#         df["solution"].unique(maintain_order=True).to_frame().with_row_count("index")
#     )

#     # every bar in the plot has a different offset for the text
#     start_offset = 10
#     offsets = [start_offset + 12 * i for i in range(0, bar_order.height)]

#     # we look for the solutions that surpassed the limit
#     # and create a text label for them
#     df = (
#         df.filter(pl.col("duration[s]") > limit)
#         .with_columns(
#             pl.when(pl.col("success"))
#             .then(
#                 pl.format(
#                     "{} took {} s", "solution", pl.col("duration[s]").cast(pl.Int32)
#                 ).alias("labels")
#             )
#             .otherwise(pl.format("{} had an internal error", "solution"))
#         )
#         .join(bar_order, on="solution")
#         .groupby("query_no")
#         .agg([pl.col("labels"), pl.col("index").min()])
#         .with_columns(pl.col("labels").arr.join(",\n"))
#     )

#     # then we create a dictionary similar to something like this:
#     #     anno_data = {
#     #         "q1": (offset, "label"),
#     #         "q3": (offset, "label"),
#     #     }

#     if df.height > 0:
#         anno_data = {
#             v[0]: (offsets[int(v[1])], v[2])
#             for v in df.select(["query_no", "index", "labels"])
#             .transpose()
#             .to_dict(False)
#             .values()
#         }
#     else:
#         # a dummy with no text
#         anno_data = {"q1": (0, "")}

#     for q_name, (x_shift, anno_text) in anno_data.items():
#         fig.add_annotation(
#             align="right",
#             x=q_name,
#             y=LIMIT,
#             xshift=x_shift,
#             yshift=30,
#             font=dict(color="white"),
#             showarrow=False,
#             text=anno_text,
#         )


def write_plot_image(fig):
    if not os.path.exists(DEFAULT_PLOTS_DIR):
        os.mkdir(DEFAULT_PLOTS_DIR)

    file_name = "tpch.html"
    fig.write_html(os.path.join(DEFAULT_PLOTS_DIR, file_name))


def plot(
    df: pd.DataFrame,
    x: str = "query_no",
    y: str = "duration[s]",
    group: str = "solution",
    limit: int = 120,
):
    """Generate a Plotly Figure of a grouped bar chart diplaying
    benchmark results from a DataFrame.

    Args:
        df (pl.DataFrame): DataFrame containing `x`, `y`, and `group`.
        x (str, optional): Column for X Axis. Defaults to "query_no".
        y (str, optional): Column for Y Axis. Defaults to "duration[s]".
        group (str, optional): Column for group. Defaults to "solution".
        limit: height limit in seconds

    Returns:
        px.Figure: Plotly Figure (histogram)
    """

    # fig = px.histogram(
    #     x=df[x],
    #     y=df[y],
    #     color=df[group],
    #     barmode=BAR_TYPE,
    #     template=DEFAULT_THEME,
    #     # color_discrete_map=COLORS,
    #     # pattern_shape=df[group],
    #     labels=LABEL_UPDATES,
    # )

    df['speedup'] = 0.0
    df = df.sort_values('solution', key=lambda s: s.apply(['pandas', 'bodo', 'polars', 'xorbits', 'modin[ray]', 'dask'].index), ignore_index=True)

    for index, row in df.iterrows():
        df.at[index, 'speedup'] = 1 / (row['duration[s]'] / df[(df['solution']=='pandas') & (df['query_no']==row['query_no'])]['duration[s]'])
    # df = df.sort_values(by='solution-version')
    # print(df.groupby('solution-version').head())
    # print(df.groupby(group).agg(
    #     {
    #         'speedup': 'mean',
    #         'solution': lambda x : x,
    #     }
    # ))
    
    avg_speedup_df = df.groupby(['solution', 'version']) \
                    .mean().reset_index() \
                    .sort_values('solution', 
                                 key=lambda s: s.apply(['pandas', 'bodo', 'polars', 'xorbits', 'modin[ray]', 'dask'].index), 
                                 ignore_index=True)[['solution', 'speedup', 'version']]
    print(avg_speedup_df)

    fig = go.Figure()

    fig.add_trace(
            go.Bar(
                x = avg_speedup_df['solution'],
                y = avg_speedup_df['speedup'],
                name = "Overall",
                customdata=avg_speedup_df,
                hovertemplate='framework: %{customdata[0]} <br>version: %{customdata[2]}<br>speedup: %{customdata[1]:.3f}x ',
                visible=True,
                marker=dict(color=[1, 2, 3, 4, 5, 6, 7], coloraxis="coloraxis"),
                showlegend=False
            ),
        )

    for i in range(1, 23):
        query = "q" + str(i)
        plot_df = df.loc[df['query_no'] == query][['solution', 'duration[s]', 'version']]
        # print(plot_df)
        fig.add_trace(
            go.Bar(
                x = plot_df['solution'],
                y = plot_df['duration[s]'],
                customdata=plot_df,
                hovertemplate='framework: %{customdata[0]} <br>version: %{customdata[2]}<br>duration: %{customdata[1]:.3f}s ',
                name = query,
                visible=False,
                marker=dict(color=[1, 2, 3, 4, 5, 6], coloraxis="coloraxis"),
                showlegend=False
            ),
        )
    
    visible_arr = [False] * 23
    visible_arr[0] = True
    button_arr = []
    overall_config = dict(label="Overall",
                 method="update",
                 args=[{"visible": copy.deepcopy(visible_arr)},
                       {"title": "Overall",}])
    visible_arr[0] = False
    button_arr.append(overall_config)

    for i in range(1, 23):
        visible_arr[i] = True
        query = "q" + str(i)
        button_config = dict(label=query,
                 method="update",
                 args=[{"visible": copy.deepcopy(visible_arr)},
                       {"title": query,}])
        button_arr.append(button_config)
        visible_arr[i] = False
    
    fig.update_layout(
        updatemenus=[
            dict(
                active=0,
                buttons=button_arr,
            )
        ])
    
    # fig.update_layout(
    #     updatemenus=[
    #         dict(
    #             active=0,
    #             buttons=list([
    #                 dict(label="Overall",
    #                     method="update",
    #                     args=[{"visible": [True, False, False, False]},
    #                         {"title": "Overall Speedup",}]),
    #                 dict(label="Q1",
    #                     method="update",
    #                     args=[{"visible": [False, True, False, False]},
    #                         {"title": "Q1",}]),
    #                 dict(label="Q2",
    #                     method="update",
    #                     args=[{"visible": [False, False, True, False]},
    #                         {"title": "Q2",}]),
    #                 dict(label="Q3",
    #                     method="update",
    #                     args=[{"visible": [False, False, False, True]},
    #                         {"title": "Q3"}]),
    #             ]),
    #         )
    #     ])
        
        # fig.add_trace(
        #     go.Bar(
        #         x = plot_df[group],
        #         y = plot_df[y],
        #         name = query,
        #         marker=dict(color=[1, 2, 3, 4, 5], coloraxis="coloraxis"),
        #         # barmode=BAR_TYPE,
        #         # template=DEFAULT_THEME,
        #         # color_discrete_map=COLORS,
        #         # pattern_shape=df[group],
        #         # labels=LABEL_UPDATES,
        #     ),
        #     row=int(q / 3) + 1, col= int(q % 3) + 1
        # )

        # print("x")
        # print("y")
        # print(df.loc[df['query_no'] == query][y].reset_index(drop=True))

    # build plotly figure object


    # fig.update_layout(
    #     barmode='group',
    #     bargroupgap=0.1,
    #     # paper_bgcolor="rgba(41,52,65,1)",
    #     # yaxis_range=[0, limit],
    #     # plot_bgcolor="rgba(41,52,65,1)",
    #     margin=dict(t=100),
    #     legend=dict(orientation="h", xanchor="left", yanchor="top", x=0.37, y=-0.1),
    # )

    # add_annotations(fig, limit, df)

    # fig.update_layout(title_text="TPC-H Benchmark")
    fig.update(layout_coloraxis_showscale=False)

    write_plot_image(fig)

    # display the object using available environment context
    fig.show()


if __name__ == "__main__":
    LIMIT = 120

    df = pd.read_csv(TIMINGS_FILE)
    df["solution-version"] = df["solution"].str.cat(df["version"], sep="-")
    df["query_no"] = "q" + df["query_no"].astype(str)

    plot(df, limit=LIMIT, group="solution-version")
