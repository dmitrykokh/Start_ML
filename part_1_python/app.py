from typing import List

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from schema import UserGet, PostGet, FeedGet
from table_feed import Feed
from table_post import Post
from table_user import User
from database import SessionLocal

app = FastAPI()


def get_db():
    with SessionLocal() as db:
        return db


@app.get("/user/{id}", response_model=UserGet)
def get_user(id, db: Session = Depends(get_db)):
    result = db.query(User).filter(User.id == id).first()
    if not result:
        raise HTTPException(404, "user not found")
    else:
        return result


@app.get("/post/{id}", response_model=PostGet)
def get_post(id, db: Session = Depends(get_db)):
    result = db.query(Post).filter(Post.id == id).first()
    if not result:
        raise HTTPException(404, "post not found")
    else:
        return result


@app.get("/user/{id}/feed", response_model=List[FeedGet])
def get_users_certain_id(id, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(Feed)\
        .filter(Feed.user_id == id)\
        .order_by(Feed.time.desc())\
        .limit(limit)\
        .all()
    if not result:
        return []
    else:
        return result


@app.get("/post/{id}/feed", response_model=List[FeedGet])
def get_posts_certain_id(id, limit: int = 10, db: Session = Depends(get_db)):
    result = db.query(Feed)\
        .filter(Feed.post_id == id)\
        .order_by(Feed.time.desc())\
        .limit(limit)\
        .all()
    if not result:
        return []
    else:
        return result


@app.get("/post/recommendations/", response_model=List[PostGet])
def get_recommendations(limit: int = 10, db: Session = Depends(get_db)):
    test = db.query(Post)\
        .select_from(Feed)\
        .filter(Feed.action == 'like')\
        .join(Post)\
        .group_by(Post.id)\
        .order_by(func.count(Post.id).desc())\
        .limit(limit)\
        .all()
    return test
