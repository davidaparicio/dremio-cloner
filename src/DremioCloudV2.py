########
# Copyright (C) 2019-2020 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
########

import logging
from DremioCloud import DremioCloud

###
# Dremio Cloud V2 (Serverless) API wrapper.
# This class extends DremioCloud to provide V2-specific configuration and handling.
# It automatically detects and configures for Dremio Cloud V2 (serverless) endpoints.
#
# Key differences from V1:
# - Serverless architecture (no fixed infrastructure)
# - Different entity handling (some fields deprecated)
# - Automatic API version detection based on endpoint
# - Cleaner separation of V1 and V2 concerns
#
###
class DremioCloudV2(DremioCloud):
	"""
	Dremio Cloud V2 (Serverless) API wrapper.
	
	This class extends DremioCloud to provide V2-specific functionality:
	- Automatic serverless mode detection
	- V2-specific entity cleaning
	- Enhanced logging for V2 operations
	- Support for V2 configuration parameters
	
	Usage:
		# For V2 endpoints (staging.dremio.site, test.dremio.site)
		dremio_v2 = DremioCloudV2(
			endpoint="https://api.staging.dremio.site/",
			username="",
			password="YOUR_PAT_TOKEN",
			org_id="YOUR_ORG_ID",
			project_id="YOUR_PROJECT_ID"
		)
		
		# For V1 endpoints (api.dremio.cloud)
		dremio_v1 = DremioCloudV2(
			endpoint="https://api.dremio.cloud/",
			username="",
			password="YOUR_PAT_TOKEN",
			org_id="YOUR_ORG_ID",
			project_id="YOUR_PROJECT_ID"
		)
	"""

	def __init__(self, endpoint, username, password, org_id, project_id, api_timeout=10, retry_timedout_source=False, verify_ssl=True):
		"""
		Initialize DremioCloudV2 instance.
		
		Args:
			endpoint: Dremio API endpoint URL
			username: Username for authentication (empty string for PAT)
			password: Password or PAT token
			org_id: Organization ID
			project_id: Project ID
			api_timeout: API request timeout in seconds (default: 10)
			retry_timedout_source: Whether to retry timed-out sources (default: False)
			verify_ssl: Whether to verify SSL certificates (default: True)
		"""
		# Call parent constructor
		super().__init__(endpoint, username, password, org_id, project_id, api_timeout, retry_timedout_source, verify_ssl)
		
		# Set serverless mode based on detected API version
		if self._api_version == "v2":
			self.set_serverless_mode(True)
			logging.info("DremioCloudV2: Initialized in serverless mode for V2 endpoint")
		else:
			self.set_serverless_mode(False)
			logging.info("DremioCloudV2: Initialized in standard mode for V1 endpoint")

	def get_api_version(self):
		"""
		Get the detected API version.
		
		Returns:
			str: "v1" for standard Dremio Cloud, "v2" for serverless
		"""
		return self._api_version

	def is_serverless(self):
		"""
		Check if this is a serverless (V2) instance.
		
		Returns:
			bool: True if serverless, False otherwise
		"""
		return self._is_serverless

	def get_endpoint_info(self):
		"""
		Get detailed endpoint information.
		
		Returns:
			dict: Dictionary containing endpoint details
		"""
		return {
			"endpoint": self._endpoint,
			"api_version": self._api_version,
			"is_serverless": self._is_serverless,
			"org_id": self._org_id,
			"project_id": self._project_id
		}

	def validate_v2_compatibility(self, entity):
		"""
		Validate if an entity is compatible with V2 (serverless) mode.
		
		Args:
			entity: Catalog entity to validate
			
		Returns:
			dict: Validation result with 'valid' (bool) and 'issues' (list)
		"""
		if entity is None:
			return {"valid": True, "issues": []}

		issues = []
		deprecated_fields = ['cloudId', 'projectStore', 'credentials', 'cloudTag', 'instanceFamily']

		for field in deprecated_fields:
			if field in entity:
				issues.append(f"Field '{field}' is deprecated in V2 and will be removed")

		return {
			"valid": len(issues) == 0,
			"issues": issues
		}

	def create_catalog_entity_v2(self, entity, dry_run=True, validate=True):
		"""
		Create a catalog entity with V2-specific handling.
		
		Args:
			entity: Catalog entity to create
			dry_run: If True, don't actually create (default: True)
			validate: If True, validate V2 compatibility first (default: True)
			
		Returns:
			dict: API response or None
		"""
		if validate and self._is_serverless:
			validation = self.validate_v2_compatibility(entity)
			if not validation["valid"]:
				logging.warning(f"V2 compatibility issues detected: {validation['issues']}")

		return self.create_catalog_entity(entity, dry_run)

	def update_catalog_entity_v2(self, entity_id, entity, dry_run=True, validate=True, report_error=True):
		"""
		Update a catalog entity with V2-specific handling.
		
		Args:
			entity_id: ID of entity to update
			entity: Updated entity data
			dry_run: If True, don't actually update (default: True)
			validate: If True, validate V2 compatibility first (default: True)
			report_error: If True, report errors (default: True)
			
		Returns:
			dict: API response or None
		"""
		if validate and self._is_serverless:
			validation = self.validate_v2_compatibility(entity)
			if not validation["valid"]:
				logging.warning(f"V2 compatibility issues detected: {validation['issues']}")

		return self.update_catalog_entity(entity_id, entity, dry_run, report_error)

	def get_v2_configuration_summary(self):
		"""
		Get a summary of V2 configuration and capabilities.
		
		Returns:
			dict: Configuration summary
		"""
		return {
			"api_version": self._api_version,
			"is_serverless": self._is_serverless,
			"endpoint": self._endpoint,
			"org_id": self._org_id,
			"project_id": self._project_id,
			"capabilities": {
				"supports_reflections": True,
				"supports_wlm": True,
				"supports_engines": self._api_version == "v2",
				"supports_arctic": self._api_version == "v2",
				"deprecated_fields_auto_cleaned": self._is_serverless
			}
		}

