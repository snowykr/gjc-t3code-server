const SYNTHETIC_PROVIDER_ID = "gajae-code";

export interface GjcSelectableModel {
  readonly provider: string;
  readonly id: string;
  readonly name?: string;
}

export function selectionInputError(model: string): string | undefined {
  if (model.trim().length === 0) return "Model selection must not be empty.";
  const prefix = `${SYNTHETIC_PROVIDER_ID}/`;
  if (model.startsWith(prefix) && model.slice(prefix.length).trim().length === 0) {
    return "Model profile selection must include a profile name.";
  }
  return undefined;
}

/** Returns a configured profile name without imposing syntax on user-owned keys. */
export function profileNameForSelection(input: {
  readonly model?: string | undefined;
  readonly modelProfile?: string | undefined;
}): string | undefined {
  const prefix = `${SYNTHETIC_PROVIDER_ID}/`;
  const profile = input.model?.startsWith(prefix) ? input.model.slice(prefix.length) : undefined;
  if (profile?.trim()) return profile;
  const configured = input.modelProfile?.trim();
  return configured || undefined;
}

/** Resolves only a concrete model exposed by the current session. */
export function findAvailableConcreteModel<T extends GjcSelectableModel>(
  models: readonly T[],
  modelId: string,
): T | undefined {
  if (profileNameForSelection({ model: modelId }) !== undefined) return undefined;
  return models.find((model) => `${model.provider}/${model.id}` === modelId);
}

export function hasSyntheticProfileNamespaceCollision(
  models: readonly Pick<GjcSelectableModel, "provider">[],
  configuredProviderIds: readonly string[],
): boolean {
  return (
    models.some((model) => model.provider === SYNTHETIC_PROVIDER_ID) ||
    configuredProviderIds.includes(SYNTHETIC_PROVIDER_ID)
  );
}
