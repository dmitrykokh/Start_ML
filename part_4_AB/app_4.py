import os
import hashlib
import pandas as pd
from datetime import datetime
from typing import Tuple
from catboost import CatBoostClassifier
from fastapi import FastAPI
from loguru import logger
from sqlalchemy import create_engine
from schema import PostGet, Response


app = FastAPI()


def batch_load_sql(query: str):
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=200000):
        chunks.append(chunk_dataframe)
        logger.info(f"Got chunk: {len(chunk_dataframe)}")
        break
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def load_raw_features():
    logger.info("loading liked posts")
    liked_posts_query = """
        SELECT distinct post_id, user_id
        FROM public.feed_data
        where action='like'"""
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info("loading posts features")
    posts_features = pd.read_sql(
        """SELECT * FROM public.posts_info_features_dl""",
        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
            "postgres.lab.karpov.courses:6432/startml"
    )

    logger.info("loading user features")
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data""",
        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
            "postgres.lab.karpov.courses:6432/startml"
    )

    return [liked_posts, posts_features, user_features]


def get_model_path(model_version: str) -> str:
    print(os.environ)
    if os.environ.get("IS_LMS") == "1":
        model_path = f"/workdir/user_input/model_{model_version}"
    else:
        model_path = f"/Users/kokh/model_{model_version}"
    return model_path


def load_models(model_version: str):
    model_path = get_model_path(model_version)
    loaded_model = CatBoostClassifier()
    loaded_model.load_model(model_path)
    return loaded_model


features = load_raw_features()

model_control = load_models("control")
model_test = load_models("test")

"""
Основная часть, где мы реализуем функцию для разбиения пользователей.
В идеале соль мы должно не задавать константой, а где-то конфигурировать.
В том числе сами границы, но сделать для простоты мы как раз разбиваем
50/50
"""

SALT = "my_salt"


def get_user_group(id: int) -> str:
    value_str = str(id) + SALT
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16)
    percent = value_num % 100
    if percent < 50:
        return "control"
    elif percent < 100:
        return "test"
    return "unknown"


def calculate_features(
    id: int, time: datetime, group: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"user_id: {id}")
    logger.info("reading features")
    user_features = features[2].loc[features[2].user_id == id]
    user_features = user_features.drop("user_id", axis=1)

    logger.info("dropping columns")
    posts_features = features[1].drop(["index", "text"], axis=1)

    logger.info("zipping everything")
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info("assigning everything")
    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index("post_id")

    logger.info("add time info")
    user_posts_features["hour"] = time.hour
    user_posts_features["month"] = time.month

    return user_features, user_posts_features


def get_recommended_feed(id: int, time: datetime, limit: int) -> Response:
    user_group = get_user_group(id=id)
    logger.info(f"user group {user_group}")

    if user_group == "control":
        model = model_control
    elif user_group == "test":
        model = model_test
    else:
        raise ValueError("unknown group")

    user_features, user_posts_features = calculate_features(
        id=id, time=time, group=user_group
    )

    logger.info("predicting")
    predicts = model.predict_proba(user_posts_features)[:, 1]
    user_posts_features["predicts"] = predicts

    logger.info("deleting liked posts")
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    recommended_posts = filtered_.sort_values("predicts")[-limit:].index

    return Response(
        recommendations=[
            PostGet(id=i[0], text="lexa", topic=i[1])
            for i in user_features.itertuples(index=False)
        ],
        exp_group=user_group,
    )


@app.get("/post/recommendations/", response_model=Response)
def recommended_posts(id: int, time: datetime, limit: int = 10) -> Response:
    return get_recommended_feed(id, time, limit)
