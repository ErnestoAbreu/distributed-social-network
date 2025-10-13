from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.friendship import Friendship
from app.models.user import User
from app.utils.auth_dependency import get_current_user

router = APIRouter(prefix="/friends", tags=["Friendships"])

@router.post("/{friend_id}")
def follow_user(
    friend_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if friend_id == current_user.id:
        raise HTTPException(status_code=400, detail="No puedes seguirte a ti mismo")
    existing = db.query(Friendship).filter(
        Friendship.user_id == current_user.id,
        Friendship.friend_id == friend_id
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Ya sigues a este usuario")

    friendship = Friendship(user_id=current_user.id, friend_id=friend_id)
    db.add(friendship)
    db.commit()
    return {"message": "Usuario seguido"}

@router.delete("/{friend_id}")
def unfollow_user(
    friend_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    friendship = db.query(Friendship).filter(
        Friendship.user_id == current_user.id,
        Friendship.friend_id == friend_id
    ).first()
    if not friendship:
        raise HTTPException(status_code=404, detail="No sigues a este usuario")

    db.delete(friendship)
    db.commit()
    return {"message": "Usuario dejado de seguir"}
