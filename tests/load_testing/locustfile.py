

import random
import string
from locust import HttpUser, task, between, tag


def random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


class FittbotUser(HttpUser):
    """
    Simulates a typical Fittbot app user making various API calls.
    """

    # Wait 1-3 seconds between tasks (simulates real user behavior)
    wait_time = between(1, 3)

    def on_start(self):
        """Called when user starts - get auth token."""
        # For load testing, use a test user token or skip auth
        self.token = "test_load_user_token"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.user_id = random.randint(1, 1000)

    # ─────────────────────────────────────────────────────────────
    # Health Checks (Always run first)
    # ─────────────────────────────────────────────────────────────

    @tag("health")
    @task(1)
    def health_check(self):
        """Basic health check."""
        self.client.get("/health/")

    @tag("health")
    @task(1)
    def queue_health(self):
        """Check Celery queue depths."""
        self.client.get("/health/celery/queues")

    # ─────────────────────────────────────────────────────────────
    # AI Tasks (Most common - 60% of traffic)
    # ─────────────────────────────────────────────────────────────

    @tag("ai", "chat")
    @task(10)
    def chat_message(self):
        """Send a chat message (most common action)."""
        with self.client.post(
            "/api/v1/chatbot/message",
            json={
                "user_id": self.user_id,
                "message": f"What should I eat today? {random_string(5)}",
                "conversation_id": random_string(8)
            },
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:
                response.success()
            elif response.status_code == 429:
                response.failure("Rate limited")
            else:
                response.failure(f"Status: {response.status_code}")

    @tag("ai", "food")
    @task(5)
    def food_log_text(self):
        """Log food via text."""
        foods = [
            "2 chapati with dal",
            "1 bowl rice with chicken curry",
            "Protein shake with banana",
            "Oats with milk and almonds"
        ]
        with self.client.post(
            "/api/v1/food/log/text",
            json={
                "user_id": self.user_id,
                "text": random.choice(foods),
                "meal_type": random.choice(["breakfast", "lunch", "dinner", "snack"])
            },
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")

    @tag("ai", "workout")
    @task(3)
    def get_workout_template(self):
        """Request workout template generation."""
        with self.client.post(
            "/api/v1/workout/generate",
            json={
                "user_id": self.user_id,
                "workout_type": random.choice(["push", "pull", "legs", "full_body"]),
                "duration_minutes": random.choice([30, 45, 60])
            },
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")

    # ─────────────────────────────────────────────────────────────
    # Payment Tasks (Less common but critical - 10% of traffic)
    # ─────────────────────────────────────────────────────────────

    @tag("payments")
    @task(2)
    def initiate_checkout(self):
        """Initiate a payment checkout."""
        with self.client.post(
            "/api/v1/payments/checkout",
            json={
                "user_id": self.user_id,
                "plan_id": random.choice(["monthly", "quarterly", "yearly"]),
                "amount": random.choice([499, 1299, 3999])
            },
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code in [200, 201, 202]:
                response.success()
            elif response.status_code == 429:
                response.failure("Rate limited")
            else:
                response.failure(f"Status: {response.status_code}")

    # ─────────────────────────────────────────────────────────────
    # Read-only endpoints (No Celery, just DB reads - 30% of traffic)
    # ─────────────────────────────────────────────────────────────

    @tag("read")
    @task(5)
    def get_user_profile(self):
        """Get user profile."""
        self.client.get(
            f"/api/v1/user/{self.user_id}/profile",
            headers=self.headers
        )

    @tag("read")
    @task(3)
    def get_food_history(self):
        """Get food logging history."""
        self.client.get(
            f"/api/v1/food/history/{self.user_id}",
            headers=self.headers
        )

    @tag("read")
    @task(2)
    def get_workout_history(self):
        """Get workout history."""
        self.client.get(
            f"/api/v1/workout/history/{self.user_id}",
            headers=self.headers
        )


class LoadTestUser(HttpUser):
    """
    Simulates load testing using mock tasks (no external APIs).
    Use this to test auto-scaling behavior.
    """

    wait_time = between(0.1, 0.5)  # Much faster than real users

    @tag("load_test", "ai")
    @task(8)
    def queue_mock_ai_task(self):
        """Queue a mock AI task."""
        with self.client.post(
            "/api/v1/load-test/ai",
            json={
                "task_id": random.randint(1, 100000),
                "duration": random.uniform(1.0, 3.0)
            },
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")

    @tag("load_test", "payments")
    @task(2)
    def queue_mock_payment_task(self):
        """Queue a mock payment task."""
        with self.client.post(
            "/api/v1/load-test/payment",
            json={
                "task_id": random.randint(1, 100000),
                "duration": random.uniform(1.0, 2.0)
            },
            catch_response=True
        ) as response:
            if response.status_code in [200, 202]:
                response.success()
            else:
                response.failure(f"Status: {response.status_code}")


class ScalingTestUser(HttpUser):
    """
    Aggressive user for testing auto-scaling triggers.
    Sends burst traffic to fill queues quickly.
    """

    wait_time = between(0.05, 0.1)  # Very aggressive

    @tag("scaling", "burst")
    @task(1)
    def burst_ai_tasks(self):
        """Send burst of AI tasks."""
        for _ in range(5):
            self.client.post(
                "/api/v1/load-test/ai",
                json={
                    "task_id": random.randint(1, 100000),
                    "duration": 3.0  # Slower tasks to build up queue
                }
            )

    @tag("scaling", "burst")
    @task(1)
    def burst_payment_tasks(self):
        """Send burst of payment tasks."""
        for _ in range(3):
            self.client.post(
                "/api/v1/load-test/payment",
                json={
                    "task_id": random.randint(1, 100000),
                    "duration": 2.0
                }
            )
