from sqlalchemy import Column, Integer, String, func

from database import Base, SessionLocal


class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    gender = Column(Integer)
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String)


if __name__ == "__main__":
    session = SessionLocal()
    query = session.query(User.country, User.os, func.count().label('count')) \
        .filter(User.exp_group == 3) \
        .group_by(User.country, User.os) \
        .having(func.count() > 100) \
        .order_by(func.count().desc())

    # Execute the query and save results in a list of tuples
    results = [(country, os, count) for country, os, count in query.all()]

    # Print the results
    final = []
    for result in results:
        final.append(result)
    print(final)
