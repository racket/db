/* Copyright 2025 Ryan Culpepper
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#include <postgres.h>
#include <libpq/oauth.h>
#include <fmgr.h>

static void startup_cb(ValidatorModuleState *state) {
  return;
}

static void shutdown_cb(ValidatorModuleState *state) {
  return;
}

static bool validate_cb(const ValidatorModuleState *state,
                        const char *token,
                        const char *role,
                        ValidatorModuleResult *result) {
  if (!strcmp(token, "valid")) {
    result->authorized = true;
    result->authn_id = pstrdup(role);
  } else {
    result->authorized = false;
    result->authn_id = NULL;
  }
  return true;
}

static const OAuthValidatorCallbacks callbacks =
  {
    PG_OAUTH_VALIDATOR_MAGIC,
    &startup_cb,
    &shutdown_cb,
    &validate_cb
  };

PG_MODULE_MAGIC;

const OAuthValidatorCallbacks *_PG_oauth_validator_module_init(void) {
  return &callbacks;
}
