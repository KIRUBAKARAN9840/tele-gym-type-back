// env.js
export const API_BASE_URL = "https://your.api.host"; // no trailing slash

// Wire this to your real auth layer (e.g., SecureStore / Redux / Context)
export async function getAuthToken() {
  return "<JWT_OR_USER_ID_FOR_DEV>";
}