from sqlalchemy.orm import Session
import models


def get_counter(db: Session):
    return db.get(models.Counter, 1)


def create_or_update_counter(db: Session, value: int):
    counter = db.get(models.Counter, 1)
    if not counter:
        counter = models.Counter(id=1, value=value)
        db.add(counter)
    else:
        counter.value = value
    db.commit()
    db.refresh(counter)
    return counter
