
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from pydantic import BaseModel
from app.models.database import get_db
from app.models.fittbot_models import Client,GymImportData, FittbotGymMembership, OldGymData, GymPlans, GymManualData
from app.utils.logging_utils import (
    auth_logger,
    FittbotHTTPException)
from typing import Optional, List

router = APIRouter(prefix="/client_data_qr", tags=["Add Client"])

@router.get("/get")
async def get_client_data_qr(id:str,plan_type:str,gym_id:int,client_id:Optional[int]=None, db:Session=Depends(get_db)):
    try:

        amount=None
        plan=None
        joining_date=None
        membership_id=None


        if plan_type =="normal":
            if not client_id:
                decrypted_uuid = id
                client = db.query(Client).filter(Client.uuid_client == decrypted_uuid).first()
                old_gym_data= db.query(OldGymData).filter(OldGymData.client_id == client.client_id,OldGymData.gym_id==gym_id).first()
            else:
                client = db.query(Client).filter(Client.client_id == client_id).first()              
                old_gym_data= db.query(OldGymData).filter(OldGymData.client_id == client_id,OldGymData.gym_id==gym_id).first()
                

            
            if client.gym_id==gym_id:
                    return{
                        "status":202,
                        "full_name":client.name,
                        "client_id":client.client_id,
                        "plan_type":plan_type,
                        "contact":client.contact,
                        "plan":plan,
                        "amount":amount,
                        "joining_date":date.today(),
                        "method":"renewal"

                    }
                
            elif old_gym_data:

                    return{
                        "status":202,
                        "full_name":client.name,
                        "client_id":client.client_id,
                        "plan_type":plan_type,
                        "plan":plan,
                        "amount":amount,
                        "joining_date":date.today(),
                        "method":"renewal",
                        "contact":client.contact,


                    }
               
        else:
            membership_card=db.query(FittbotGymMembership).filter(FittbotGymMembership.id == id).first()
            if membership_card :
                client_id=membership_card.client_id
                plan_id=membership_card.plan_id
                membership_gym_id=membership_card.gym_id
                if gym_id != int(membership_gym_id):
                    return {
                        "status":402,
                        "message":"This membership card does not belong to this gym",
                    }

                client = db.query(Client).filter(Client.client_id == client_id).first()
                old_gym_data= db.query(OldGymData).filter(OldGymData.client_id == client_id,OldGymData.gym_id==gym_id).first()
                print("old gym dats",old_gym_data)
                print("client iu",client_id)
                print("gym_id",gym_id)
                print("hjshd",client.gym_id)

                plans=db.query(GymPlans).filter(GymPlans.id == plan_id).first()
                if plans:
                    plan=plans.id
                    amount=membership_card.amount

                if client.gym_id==gym_id:
                    print("sjgjsgjsg")
                    return{
                        "status":201,
                        "membership_id":id,
                        "full_name":client.name,
                        "client_id":client.client_id,
                        "plan_type":plan_type,
                        "plan":plan,
                        "amount":amount,
                        "joining_date":date.today(),
                        "method":"renewal"

                    }
                
                elif old_gym_data:
                    print("thalaa")

                    return{
                        "status":201,
                        "membership_id":id,
                        "full_name":client.name,
                        "client_id":client.client_id,
                        "plan_type":plan_type,
                        "plan":plan,
                        "amount":amount,
                        "joining_date":date.today(),
                        "method":"renewal"

                    }
               
                membership_id=membership_card.id
            else:
                return HTTPException(status_code=404, detail="Membership card not found")
            

        if not client:
            raise HTTPException(status_code=404, detail="User not found")
        
        response={
            "membership_id":membership_id,
            "goals":client.goals,
            "bmi":client.bmi,
            "height":client.height,
            "age":client.age,
            "lifestyle":client.lifestyle,
            "weight":client.weight,
            "profile":client.profile,
            "full_name":client.name,
            "client_id":client.client_id,
            "contact":client.contact,
            "email":client.email,
            "medical_issues":client.medical_issues,
            "gender":client.gender,
            "dob":client.dob,
            "uuid":client.uuid_client,
            "plan":plan,
            "amount":amount,
            "joining_date":date.today()
        }

        return{
            "status":200,
            "message":"Data retrived successfully",
            "data":response
        }
    
    except HTTPException:
        raise
    
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occured:{str(e)}")



@router.get("/form")
async def get_form_data(gym_id:int,db:Session=Depends(get_db)):
    try:
        eligibility= db.query(GymManualData).filter(GymManualData.gym_id==gym_id).first()
        if eligibility:
            return{
                "status":200,
                "eligibility":True
            }
        else:
            return{
                "status":200,
                "eligibility":False
            }
        
    
        
    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        print(f"Unexpected error: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )
    



class AddClientManualPayload(BaseModel):
    full_name: str
    mobile: str
    email: str
    location: str
    gender: str
    fee_status: Optional[str] = None
    admission_number: Optional[str] = None
    expiry_date: Optional[date] = None


class AddClientManual(BaseModel):
    gym_id: int
    import_type: Optional[str] = None
    clients: List[AddClientManualPayload]


@router.post("/add_client")
async def add_client_manually(addRequest:AddClientManual, db:Session=Depends(get_db)):
    try:
        if not addRequest.clients:
            raise HTTPException(status_code=400, detail="No clients received")

        import_type = addRequest.import_type or "manual"

        for client in addRequest.clients:
            add_gym_data = GymImportData(
                gym_id=addRequest.gym_id,
                client_name=client.full_name,
                client_contact=client.mobile,
                client_email=client.email,
                client_location=client.location,
                status=client.fee_status,
                gender=client.gender,
                admission_number=client.admission_number,
                expires_at=client.expiry_date,
                import_type=import_type
            )
            db.add(add_gym_data)

        db.commit()

        return{
            "status":200,
            "imported_count": len(addRequest.clients)
        }

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        print(f"Unexpected error: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )



class AddClientManualForm(BaseModel):
    gym_id:int
    total_clients:int
    active_clients:int
    inactive_clients:int
    total_enquiries: int
    total_followups: int


@router.post("/add_form")
async def add_client_manually(addRequest:AddClientManualForm, db:Session=Depends(get_db)):
    try:
        add_gym_data= GymManualData(
            gym_id=addRequest.gym_id,
            total_clients=addRequest.total_clients,
            active_clients=addRequest.active_clients,
            inactive_clients=addRequest.inactive_clients,
            total_enquiries=addRequest.total_enquiries,
            total_followups=addRequest.total_followups
            )
        
        db.add(add_gym_data)
        db.commit()

        return{
            "status":200
        }

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        print(f"Unexpected error: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )

    
