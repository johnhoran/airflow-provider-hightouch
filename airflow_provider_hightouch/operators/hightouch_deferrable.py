from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_provider_hightouch.hooks.hightouch import HightouchHook  # type: ignore
from airflow_provider_hightouch.triggers import HightouchTrigger


class HightouchDeferrableSyncOperator(BaseOperator):
    """
    Deferable version of HightouchDeferrableSyncOperator.

    This operator triggers a Hightouch sync and defers execution until the sync is completed.
    """

    def __init__(
        self,
        workspace_id: Optional[str],
        sync_id: Optional[str] = None,
        sync_slug: Optional[str] = None,
        connection_id: str = "hightouch_default",
        api_version: str = "v3",
        error_on_warning: bool = False,
        poll_interval: float = 15,
        timeout: int = 3600,
        **kwargs,
    ):
        """
        Initializes the HightouchDeferrableSyncOperator.

        Args:
            workspace_id (Optional[str]): The Hightouch workspace_id.
            sync_id (Optional[str]): The ID of the sync to trigger.
            sync_slug (Optional[str]): The slug of the sync to trigger.
            connection_id (str): The connection ID for Hightouch.
            api_version (str): The API version to use.
            error_on_warning (bool): If True, raise an error on warnings.
            poll_interval (float): The interval to poll for sync completion.
            timeout (int): The maximum time to wait for the sync to complete.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(**kwargs)
        self.hightouch_conn_id = connection_id
        self.api_version = api_version
        self.sync_id = sync_id
        self.sync_slug = sync_slug
        self.workspace_id = workspace_id
        self.error_on_warning = error_on_warning
        self.poll_interval = poll_interval
        self.timeout = timeout

    def execute(self, context: Context):
        """
        Starts the Hightouch sync and defers execution.

        This method triggers the Hightouch sync asynchronously and defers execution
        until the sync is completed. It requires either a sync_id or sync_slug to
        initiate the sync process.

        Args:
            context (Context): The context in which the operator is executed.

        Raises:
            AirflowException: If neither sync_id nor sync_slug is provided, or if both are provided.
        """
        hook = HightouchHook(hightouch_conn_id=self.hightouch_conn_id)

        # Validate that exactly one of sync_id or sync_slug is provided
        if (self.sync_id is None) == (self.sync_slug is None):
            raise AirflowException("Exactly one of sync_id or sync_slug must be provided.")

        if self.sync_slug:
            self.log.info(f"Triggering sync asynchronously using slug ID: {self.sync_slug}...")

            sync_request_id = hook.start_sync(sync_slug=self.sync_slug)
            self.sync_id = hook.get_sync_from_slug(sync_slug=self.sync_slug)  # Retrieve sync_id based on slug
        else:
            self.log.info(f"Triggering sync asynchronously using sync ID: {self.sync_id}...")
            sync_request_id = hook.start_sync(sync_id=self.sync_id)

        self.log.info(f"Successfully started sync {self.sync_id}. Deferring execution...")

        self.defer(
            trigger=HightouchTrigger(
                workspace_id=self.workspace_id,
                sync_id=self.sync_id,
                sync_request_id=sync_request_id,
                sync_slug=self.sync_slug,
                connection_id=self.hightouch_conn_id,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """
        Resumes execution after the trigger completes.

        This method is called after the Hightouch sync completes. It processes the
        event containing the sync status and raises exceptions if the sync fails or
        times out.

        Args:
            context (Context): The context in which the operator is executed.
            event (dict): The event containing the sync status.

        Raises:
            AirflowException: If the sync fails or times out.
        """
        status = event.get("status")
        message = event.get("message")

        if status == "success":
            self.log.info(message)
            return event.get("sync_id", None)
        elif status in ["failed", "timeout"]:
            raise AirflowException(message)
        else:
            raise AirflowException(message)