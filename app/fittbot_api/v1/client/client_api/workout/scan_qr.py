from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.fittbot_models import QRCode
from app.utils.logging_utils import FittbotHTTPException
from typing import Dict,Any


router = APIRouter(prefix="/qr", tags=["scanqr"])




QR_LINK_CONFIG: Dict[str, Dict[str, Any]] = {
    "https://qr1.be/JKBC": {
        "equipment": "Rods Exercises",
        "ids": [46, 47, 48, 60, 61, 66, 68, 69, 71, 76, 81, 104, 105, 106, 132, 133, 177, 178, 179, 192, 193, 199],
    },
    "https://qr1.be/P6Z3": {
        "equipment": "Static Bench Exercises",
        "ids": [1, 2, 3, 6, 10, 17, 145, 146, 154, 181, 210],
    },
    "https://qr1.be/74QP": {
        "equipment": "Dumbbell Exercises",
        "ids": [8, 42, 44, 45, 59, 70, 73, 77, 99, 100, 101, 102, 103, 130, 131, 158, 174, 175, 176, 191, 197, 198, 202, 203, 204, 205, 206],
    },
    "https://qr1.be/PJYA": {
        "equipment": "Cardio Exercises",
        "ids": [82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98],
    },
    "https://qr1.be/AG98": {
        "equipment": "Spin Bike",
        "ids": [163],
    },
    "https://qr1.be/B52G": {
        "equipment": "Recumbent Bike",
        "ids": [162],
    },
    "https://qr1.be/EF7D": {
        "equipment": "Upright Bike",
        "ids": [164],
    },
    "https://qr1.be/DVBL": {
        "equipment": "Treadmill",
        "ids": [195],
    },
    "https://qr1.be/9HCX": {
        "equipment": "Chest Press Machine",
        "ids": [117, 118, 119, 186],
    },
    "https://qr1.be/ULBM": {
        "equipment": "Chest Dips Machine",
        "ids": [114],
    },
    "https://qr1.be/VOLC": {
        "equipment": "Seated Calf Raise Machine",
        "ids": [182],
    },
    "https://qr1.be/YUC9": {
        "equipment": "Wrist Curls Machine",
        "ids": [165, 166, 167, 169, 170, 171, 172, 173],
    },
    "https://qr1.be/MQJL": {
        "equipment": "Barbell Chest Press Machine",
        "ids": [],
    },
    "https://qr1.be/AKGI": {
        "equipment": "Lat Pull Down Machine",
        "ids": [50, 51, 52, 53],
    },
    "https://qr1.be/8KUG": {
        "equipment": "Machine Leg Curls",
        "ids": [],
    },
    "https://qr1.be/8PL6": {
        "equipment": "Cable Crossover Machine",
        "ids": [12, 49, 54, 62, 63, 65, 67, 78, 80, 110, 111, 112, 113, 115, 123, 124, 125, 126, 127, 129, 150, 157, 200, 209],
    },
    "https://qr1.be/J2BJ": {
        "equipment": "Leg Press Machine",
        "ids": [184, 185],
    },
    "https://qr1.be/G52K": {
        "equipment": "Smith Machine",
        "ids": [55, 168, 183, 194],
    },
    "https://qr1.be/8H16": {
        "equipment": "V-Squats Machine",
        "ids": [],
    },
    "https://qr1.be/K1SB": {
        "equipment": "Fly/Pec Machine",
        "ids": [116],
    },
}


class scanqr(BaseModel):
    link: str
    gender: str
    muscle_group:list


@router.get("/get")
async def get_all_exercises_by_gender(gender: str, db: Session = Depends(get_db)):

    try:
        # Determine which gender paths to use
        gender_lower = gender.lower()
        use_female_path = gender_lower in ["female", "f"]

        # Get all exercises
        all_exercises = db.query(QRCode).all()

        if not all_exercises:
            raise FittbotHTTPException(
                status_code=404,
                detail="No exercises found in database.",
                error_code="NO_EXERCISES_FOUND",
                log_data={"gender": gender},
            )

        response_data = {}
        for record in all_exercises:
            group = record.muscle_group
            if group not in response_data:
                response_data[group] = {
                    "exercises": [],
                    "isMuscleGroup": False,
                    "isCardio": False,
                }

            # Select the appropriate gif and img paths based on gender
            gif_path = record.gif_path_f if use_female_path else record.gif_path_m
            img_path = record.img_path_f if use_female_path else record.img_path_m

            response_data[group]["exercises"].append({
                "id": record.id,
                "name": record.exercises,
                "gifPath": gif_path,
                "imgPath": img_path,
                "gifPathMale": record.gif_path_m,
                "gifPathFemale": record.gif_path_f,
                "imgPathMale": record.img_path_m,
                "imgPathFemale": record.img_path_f,
                "isMuscleGroup": record.isMuscleGroup,
                "isCardio": record.isCardio,
                "isBodyWeight": record.isBodyWeight,
                "muscleGroup": record.muscle_group,
            })
            response_data[group]["isMuscleGroup"] = record.isMuscleGroup
            response_data[group]["isCardio"] = record.isCardio

        return {"status": 200, "data": response_data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching exercises: {str(e)}",
            error_code="GET_EXERCISES_ERROR",
            log_data={"gender": gender, "error": str(e)},
        )


@router.post("/scan")
async def get_grouped_exercises(req: scanqr, db: Session = Depends(get_db)):
    try:
        link = req.link
        muscle_group= req.muscle_group
        
        print("link is", link)
        print("muscle_group is", muscle_group)

        config = QR_LINK_CONFIG.get(link)
        if not config:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"No equipment configured for link {link!r}",
                error_code="QR_LINK_NOT_CONFIGURED",
                log_data={"link": link},
            )

        ids = config["ids"]

        # Filter by IDs and muscle groups
        query = db.query(QRCode).filter(QRCode.id.in_(ids))

        # If muscle_group list is provided and not empty, filter by muscle groups
        if muscle_group and len(muscle_group) > 0:
            query = query.filter(QRCode.muscle_group.in_(muscle_group))

        records = query.all()
        if not records:
            raise FittbotHTTPException(
                status_code=404,
                detail="No records found with given ids and muscle groups.",
                error_code="QR_CODES_NOT_FOUND",
                log_data={"ids": ids, "muscle_groups": muscle_group},
            )

        # Determine which gender paths to use
        gender = req.gender.lower()
        use_female_path = gender in ["female", "f"]

        response_data = {}
        for record in records:
            group = record.muscle_group
            if group not in response_data:
                response_data[group] = {
                    "exercises": [],
                    "isMuscleGroup": False,
                    "isCardio": False,
                }

            # Select the appropriate gif and img paths based on gender
            gif_path = record.gif_path_f if use_female_path else record.gif_path_m
            img_path = record.img_path_f if use_female_path else record.img_path_m

            response_data[group]["exercises"].append({
                "id": record.id,
                "name": record.exercises,
                "gifPath": gif_path,
                "imgPath": img_path,
                "gifPathMale": record.gif_path_m,
                "gifPathFemale": record.gif_path_f,
                "imgPathMale": record.img_path_m,
                "imgPathFemale": record.img_path_f,
                "isMuscleGroup": record.isMuscleGroup,
                "isCardio": record.isCardio,
                "isBodyWeight": record.isBodyWeight,
                "muscleGroup": record.muscle_group,
            })
            response_data[group]["isMuscleGroup"] = record.isMuscleGroup
            response_data[group]["isCardio"] = record.isCardio

        print(response_data)
        return {"status": 200, "data": response_data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching grouped exercises:{str(e)}",
            error_code="QR_SCAN_ERROR",
            log_data={"link": req.link, "error": str(e)},
        )
