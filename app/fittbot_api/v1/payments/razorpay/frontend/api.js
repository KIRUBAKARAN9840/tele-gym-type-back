// api.js
import { API_BASE_URL, getAuthToken } from "./env";

async function http(method, path, body, signal) {
  const token = await getAuthToken();
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) {
    const msg = data?.detail || data?.message || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

export const PaymentsAPI = {
  // Create gym membership order (backend will lookup price/duration from gym_plans)
  createGymOrder: (gym_id, plan_id, start_on) =>
    http("POST", "/payments/gym/checkout/create-order", { gym_id, plan_id, start_on }),

  // Verify gym payment after Razorpay checkout
  verifyGymPayment: (razorpay_payment_id, razorpay_order_id, razorpay_signature) =>
    http("POST", "/payments/gym/checkout/verify", {
      razorpay_payment_id,
      razorpay_order_id,
      razorpay_signature
    }),
};