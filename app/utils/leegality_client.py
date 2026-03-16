"""
Leegality E-Sign Client for Gym Onboarding Agreements

Enterprise-grade integration with Leegality Smart API for digital document signing.
Handles document creation, signature workflows, and document downloads.

Features:
- Singleton pattern for connection pooling
- Automatic retry on network errors and 5xx responses
- Document signing workflows
- Signed document and audit trail downloads
- Proper timeout configuration with separate connect/read/write/pool timeouts

Usage:
    client = LeegalityClient.get_instance()
    result = await client.send_gym_agreement(...)
    pdf = await client.download_signed_document(document_id, request_id)
"""
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import httpx

from app.config.settings import settings

logger = logging.getLogger("leegality")


class LeegalityError(Exception):
    """Base exception for Leegality integration."""


class LeegalityClient:
    """
    Async client for Leegality Smart API v3.0.

    Features:
    - Singleton pattern for connection pooling
    - Automatic retry on network errors and 5xx
    - Document signing workflows
    - Signed document and audit trail downloads
    """

    _instance: Optional["LeegalityClient"] = None
    _client: Optional[httpx.AsyncClient] = None

    def __init__(self):
        """
        Initialize the Leegality client with settings validation.

        Note: Logs warnings if settings are missing but doesn't raise.
        This allows the app to start even if Leegality is not configured.
        """
        self._base_url = settings.leegality_base_url
        self._auth_token = settings.leegality_auth_token
        self._profile_id = settings.leegality_profile_id

        # Validate required settings (log warning, don't raise in init)
        if not self._auth_token:
            logger.warning("LEEGALITY_AUTH_TOKEN is not set - API calls will fail")
        if not self._profile_id:
            logger.warning("LEEGALITY_PROFILE_ID is not set - API calls will fail")
        if not self._base_url:
            logger.warning("LEEGALITY_BASE_URL is not set - API calls will fail")

        # Use separate timeouts for different operations
        self._timeout = httpx.Timeout(
            connect=5.0,      # Connection timeout
            read=30.0,        # Read timeout for API calls
            write=10.0,       # Write timeout
            pool=5.0          # Pool timeout
        )

        # Shorter timeout for downloads (CDN URLs expire in 15 seconds)
        self._download_timeout = httpx.Timeout(
            connect=5.0,
            read=15.0,
            write=10.0,
            pool=5.0
        )

        logger.info(
            "LeegalityClient initialized",
            extra={
                "base_url": self._base_url,
                "profile_id": self._profile_id,
                "has_auth_token": bool(self._auth_token),
            }
        )

    @classmethod
    def get_instance(cls) -> "LeegalityClient":
        """Get singleton instance of the client."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        if cls._instance is not None:
            cls._instance = None
            cls._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        """
        Get or create the shared async client with connection pooling.

        Returns:
            httpx.AsyncClient: Configured async HTTP client
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                limits=httpx.Limits(
                    max_connections=100,        # Total connections
                    max_keepalive_connections=20  # Reusable connections
                ),
            )
        return self._client

    async def aclose(self):
        """
        Close the async client.

        Should be called on app shutdown via FastAPI lifespan events.
        """
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
            logger.info("LeegalityClient closed")

    def _validate_config(self) -> None:
        """
        Validate that all required configuration is present.

        Raises:
            LeegalityError: If required settings are missing
        """
        if not self._auth_token:
            raise LeegalityError("LEEGALITY_AUTH_TOKEN is not configured")
        if not self._profile_id:
            raise LeegalityError("LEEGALITY_PROFILE_ID is not configured")
        if not self._base_url:
            raise LeegalityError("LEEGALITY_BASE_URL is not configured")

    # ---------- Public API ----------

    async def health_check(self, request_id: str) -> bool:
        """
        Verify API connectivity and authentication.

        Useful for startup checks and monitoring.

        Args:
            request_id: Request ID for logging

        Returns:
            True if API is accessible and authentication works
        """
        try:
            self._validate_config()
        except LeegalityError:
            logger.warning("Leegality health check failed - missing configuration")
            return False

        headers = {
            "X-Auth-Token": self._auth_token,
            "X-Request-Id": request_id,
        }

        client = await self._get_client()

        try:
            # Make a lightweight API call
            resp = await client.get(
                "/api/v3.0/sign/request",
                params={"documentId": "health-check-dummy"},
                headers=headers,
                timeout=5.0
            )

            # We expect 200 with status=0 (document not found), which means auth is OK
            if resp.status_code == 200:
                data = resp.json()
                # status=0 means "not found" which is expected, auth is working
                if data.get("status") in [0, 1]:
                    logger.info("Leegality health check passed")
                    return True

            logger.warning(
                "Leegality health check failed",
                extra={"status": resp.status_code}
            )
            return False

        except Exception as exc:
            logger.error(
                "Leegality health check exception",
                extra={"error": str(exc)}
            )
            return False

    async def send_gym_agreement(
        self,
        *,
        gym_name: str,
        location: str,
        gst_no: str,
        pan: str,
        address: str,
        authorised_name: str,
        mobile: str,
        email: str,
        internal_reference: str,
        request_id: str,
    ) -> Dict[str, Any]:
        """
        Create and send gym agreement for e-signature.

        Args:
            gym_name: Name of the gym
            location: Gym location
            gst_no: GST number
            pan: PAN number
            address: Full address
            authorised_name: Name of authorised signatory
            mobile: Mobile number (10 digits)
            email: Email address
            internal_reference: Internal reference number (IRN)
            request_id: Request ID for tracing

        Returns:
            Dict containing:
                - documentId: Leegality document ID
                - signUrl: URL for signer to sign document
                - irn: Internal reference number
                - folderId: Folder ID in Leegality
                - expiryDate: When the signing link expires
                - inviteeName/inviteeEmail/inviteePhone: Signer details

        Raises:
            LeegalityError: If API call fails
        """
        self._validate_config()

        payload = self._build_run_workflow_payload(
            gym_name=gym_name,
            location=location,
            gst_no=gst_no,
            pan=pan,
            address=address,
            authorised_name=authorised_name,
            mobile=mobile,
            email=email,
            internal_reference=internal_reference,
        )

        headers = {
            "X-Auth-Token": self._auth_token,
            "Content-Type": "application/json",
            "X-Request-Id": request_id,
        }

        # Enterprise-style: limited retries on network/5xx only
        max_retries = 2
        backoff_seconds = 1.5

        client = await self._get_client()

        for attempt in range(max_retries + 1):
            try:
                logger.info(
                    "Calling Leegality API",
                    extra={
                        "request_id": request_id,
                        "attempt": attempt,
                        "irn": internal_reference,
                    },
                )
                resp = await client.post(
                    "/api/v3.0/sign/request",
                    json=payload,
                    headers=headers,
                )
            except httpx.RequestError as exc:
                logger.warning(
                    "Leegality network error",
                    extra={"error": str(exc), "request_id": request_id, "attempt": attempt},
                )
                if attempt < max_retries:
                    await asyncio.sleep(backoff_seconds * (attempt + 1))
                    continue
                raise LeegalityError("Network error while calling Leegality") from exc

            # Got HTTP response, handle status
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                except ValueError as exc:
                    logger.error(
                        "Invalid JSON from Leegality",
                        extra={"status": resp.status_code, "body": resp.text[:500]},
                    )
                    raise LeegalityError("Invalid JSON from Leegality") from exc

                # Check Leegality's status field (status=1 means success)
                api_status = data.get("status")
                if api_status != 1:
                    error_messages = data.get("messages", [])
                    error_msg = (
                        error_messages[0].get("message")
                        if error_messages
                        else "Unknown error from Leegality"
                    )
                    logger.error(
                        "Leegality API returned error status",
                        extra={
                            "request_id": request_id,
                            "status": api_status,
                            "messages": error_messages,
                        },
                    )
                    raise LeegalityError(f"Leegality API error: {error_msg}")

                # Extract from correct response structure
                # Leegality returns: {"status": 1, "data": {"documentId": "...", "invitees": [{"signUrl": "..."}]}}
                response_data = data.get("data", {})
                document_id = response_data.get("documentId")
                irn = response_data.get("irn")
                folder_id = response_data.get("folderId")

                # Use "invitees" field (correct for API 3.0)
                invitees_list = response_data.get("invitees", [])

                if not invitees_list:
                    logger.error(
                        "No invitees in Leegality response",
                        extra={"request_id": request_id, "response": data},
                    )
                    raise LeegalityError("No invitees returned in Leegality response")

                # Get first invitee's signing details
                first_invitee = invitees_list[0]
                sign_url = first_invitee.get("signUrl")
                expiry_date = first_invitee.get("expiryDate")

                if not sign_url:
                    logger.error(
                        "No signUrl in invitee data",
                        extra={"request_id": request_id, "invitee": first_invitee},
                    )
                    raise LeegalityError("No signing URL returned by Leegality")

                logger.info(
                    "Leegality document created successfully",
                    extra={
                        "request_id": request_id,
                        "document_id": document_id,
                        "irn": irn,
                    },
                )

                return {
                    "documentId": document_id,
                    "signUrl": sign_url,
                    "irn": irn,
                    "folderId": folder_id,
                    "expiryDate": expiry_date,
                    "inviteeName": first_invitee.get("name"),
                    "inviteeEmail": first_invitee.get("email"),
                    "inviteePhone": first_invitee.get("phone"),
                    "active": first_invitee.get("active"),
                    "raw_response": data,  # Keep for debugging
                }

            # 4xx - client error, don't retry
            if 400 <= resp.status_code < 500:
                logger.error(
                    "Leegality returned 4xx",
                    extra={
                        "request_id": request_id,
                        "status": resp.status_code,
                        "body": resp.text[:1000],
                    },
                )
                raise LeegalityError(
                    f"Leegality returned client error {resp.status_code}: {resp.text}"
                )

            # 5xx - server error, retry
            logger.warning(
                "Leegality 5xx error",
                extra={
                    "request_id": request_id,
                    "status": resp.status_code,
                    "body": resp.text[:500],
                    "attempt": attempt,
                },
            )
            if attempt < max_retries:
                await asyncio.sleep(backoff_seconds * (attempt + 1))
                continue
            raise LeegalityError(
                f"Leegality returned server error {resp.status_code}: {resp.text}"
            )

        # Should not reach here
        raise LeegalityError("Unexpected error in Leegality client")

    async def download_signed_document(self, document_id: str, request_id: str) -> bytes:
        """
        Download signed PDF document from Leegality using fetchDocument API.

        Uses the correct endpoint: /api/v3.1/document/fetchDocument
        Returns binary PDF content directly.

        Args:
            document_id: Leegality document ID
            request_id: Request ID for tracing

        Returns:
            PDF content as bytes

        Raises:
            LeegalityError: If download fails
        """
        self._validate_config()

        headers = {
            "X-Auth-Token": self._auth_token,
            "X-Request-Id": request_id,
        }

        params = {
            "documentId": document_id,
            "documentDownloadType": "DOCUMENT",  # Required parameter
        }

        client = await self._get_client()

        try:
            logger.info(
                "Downloading signed document from Leegality",
                extra={"document_id": document_id, "request_id": request_id}
            )

            # Use download timeout (shorter for CDN URLs)
            resp = await client.get(
                "/api/v3.1/document/fetchDocument",
                params=params,
                headers=headers,
                timeout=self._download_timeout,
            )

            if resp.status_code == 200:
                pdf_content = resp.content

                # Validate it's actually a PDF
                if not pdf_content.startswith(b'%PDF'):
                    logger.error(
                        "Downloaded content is not a valid PDF",
                        extra={
                            "document_id": document_id,
                            "content_start": str(pdf_content[:100]),
                        }
                    )
                    raise LeegalityError("Downloaded content is not a valid PDF")

                logger.info(
                    "Document downloaded successfully",
                    extra={
                        "document_id": document_id,
                        "size_bytes": len(pdf_content),
                        "request_id": request_id,
                    }
                )
                return pdf_content

            # Handle specific error responses
            logger.error(
                "Failed to download document",
                extra={
                    "document_id": document_id,
                    "status": resp.status_code,
                    "body": resp.text[:500],
                    "request_id": request_id,
                },
            )

            if resp.status_code == 404:
                raise LeegalityError(f"Document not found: {document_id}")
            elif resp.status_code == 401:
                raise LeegalityError("Authentication failed - check API token")
            elif resp.status_code == 400:
                raise LeegalityError(f"Invalid request: {resp.text}")
            else:
                raise LeegalityError(f"Failed to download document: HTTP {resp.status_code}")

        except httpx.RequestError as exc:
            logger.error(
                "Network error downloading document",
                extra={"document_id": document_id, "error": str(exc), "request_id": request_id},
            )
            raise LeegalityError("Network error downloading document") from exc

    async def download_audit_trail(self, document_id: str, request_id: str) -> bytes:
        """
        Download audit trail PDF for a completed document.

        Args:
            document_id: Leegality document ID
            request_id: Request ID for tracing

        Returns:
            Audit trail PDF content as bytes

        Raises:
            LeegalityError: If download fails or document not completed
        """
        self._validate_config()

        headers = {
            "X-Auth-Token": self._auth_token,
            "X-Request-Id": request_id,
        }

        params = {
            "documentId": document_id,
            "documentDownloadType": "AUDIT_TRAIL",
        }

        client = await self._get_client()

        try:
            logger.info(
                "Downloading audit trail from Leegality",
                extra={"document_id": document_id, "request_id": request_id}
            )

            resp = await client.get(
                "/api/v3.1/document/fetchDocument",
                params=params,
                headers=headers,
                timeout=self._download_timeout,
            )

            if resp.status_code == 200:
                pdf_content = resp.content

                if not pdf_content.startswith(b'%PDF'):
                    raise LeegalityError("Downloaded audit trail is not a valid PDF")

                logger.info(
                    "Audit trail downloaded successfully",
                    extra={
                        "document_id": document_id,
                        "size_bytes": len(pdf_content),
                        "request_id": request_id,
                    }
                )
                return pdf_content

            logger.error(
                "Failed to download audit trail",
                extra={
                    "document_id": document_id,
                    "status": resp.status_code,
                    "body": resp.text[:500],
                    "request_id": request_id,
                },
            )

            if resp.status_code == 400:
                raise LeegalityError(
                    "Audit trail not available - document may not be completed yet"
                )
            elif resp.status_code == 404:
                raise LeegalityError(f"Document not found: {document_id}")
            elif resp.status_code == 401:
                raise LeegalityError("Authentication failed - check API token")
            else:
                raise LeegalityError(
                    f"Failed to download audit trail: HTTP {resp.status_code}"
                )

        except httpx.RequestError as exc:
            logger.error(
                "Network error downloading audit trail",
                extra={"document_id": document_id, "error": str(exc), "request_id": request_id},
            )
            raise LeegalityError("Network error while downloading audit trail") from exc

    async def get_document_status(self, document_id: str, request_id: str) -> Dict[str, Any]:
        """
        Check the status of a document.

        Args:
            document_id: Leegality document ID
            request_id: Request ID for tracing

        Returns:
            Dict containing document status and details

        Raises:
            LeegalityError: If status check fails
        """
        self._validate_config()

        headers = {
            "X-Auth-Token": self._auth_token,
            "X-Request-Id": request_id,
        }

        params = {"documentId": document_id}

        client = await self._get_client()

        try:
            logger.info(
                "Checking document status",
                extra={"document_id": document_id, "request_id": request_id}
            )

            resp = await client.get(
                "/api/v3.0/sign/request",
                params=params,
                headers=headers,
            )

            if resp.status_code == 200:
                data = resp.json()

                if data.get("status") != 1:
                    error_messages = data.get("messages", [])
                    error_msg = (
                        error_messages[0].get("message")
                        if error_messages
                        else "Unknown error"
                    )
                    raise LeegalityError(f"Status check failed: {error_msg}")

                logger.info(
                    "Document status retrieved",
                    extra={"document_id": document_id, "request_id": request_id}
                )

                return data.get("data", {})

            logger.error(
                "Failed to get document status",
                extra={
                    "document_id": document_id,
                    "status": resp.status_code,
                    "body": resp.text[:500],
                    "request_id": request_id,
                },
            )
            raise LeegalityError(
                f"Failed to get document status: HTTP {resp.status_code}"
            )

        except httpx.RequestError as exc:
            logger.error(
                "Network error checking document status",
                extra={"document_id": document_id, "error": str(exc), "request_id": request_id},
            )
            raise LeegalityError("Network error while checking document status") from exc

    # ---------- Internals ----------

    def _build_run_workflow_payload(
        self,
        *,
        gym_name: str,
        location: str,
        gst_no: str,
        pan: str,
        address: str,
        authorised_name: str,
        mobile: str,
        email: str,
        internal_reference: str,
    ) -> Dict[str, Any]:
        """
        Build the payload for Leegality API v3.0 workflow execution.

        Maps gym onboarding data to template fields.
        """
        today = datetime.now()

        # Build file fields array with template field IDs
        fields = [
            {"id": "1766401461675", "name": "day", "value": str(today.day), "type": "text", "required": False},
            {"id": "1766401533405", "name": "Month", "value": today.strftime("%B"), "type": "text", "required": False},
            {"id": "1766401570449", "name": "gym name", "value": gym_name, "type": "text", "required": False},
            {"id": "1766401600175", "name": "location", "value": location, "type": "text", "required": False},
            {"id": "1766401624429", "name": "gstno", "value": gst_no or "N/A", "type": "text", "required": False},
            {"id": "1766401647884", "name": "pan", "value": pan or "N/A", "type": "text", "required": False},
            {"id": "1766401715516", "name": "address", "value": address, "type": "text", "required": False},
            {"id": "1766401739224", "name": "Address", "value": address, "type": "text", "required": False},
            {"id": "1766401757915", "name": "authorised name", "value": authorised_name, "type": "text", "required": False},
            {"id": "1766401807490", "name": "mobile", "value": mobile, "type": "text", "required": False},
            {"id": "1766401830587", "name": "maild", "value": email, "type": "text", "required": False},
        ]

        payload: Dict[str, Any] = {
            "profileId": self._profile_id,
            "file": {
                "name": f"Gym_Agreement_{gym_name}_{internal_reference}",
                "fields": fields,
            },
            "invitees": [
                {
                    "name": authorised_name,
                    "email": email,
                    "phone": mobile,  # 10 digits, no country code
                }
            ],
            "irn": internal_reference,
        }

        return payload


# Convenience function for getting the client
def get_leegality_client() -> LeegalityClient:
    """Get the singleton Leegality client instance."""
    return LeegalityClient.get_instance()
