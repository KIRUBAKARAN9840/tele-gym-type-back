// useGymMembershipCheckout.js
import { useCallback, useRef, useState } from "react";
import RazorpayCheckout from "react-native-razorpay";
import { PaymentsAPI } from "./api";

export function useGymMembershipCheckout() {
  const [busy, setBusy] = useState(false);
  const abortRef = useRef(null);

  const cancel = useCallback(() => {
    abortRef.current?.abort?.();
    abortRef.current = null;
    setBusy(false);
  }, []);

  const start = useCallback(async (opts) => {
    if (busy) return { ok: false, error: "Payment is already in progress." };
    setBusy(true);
    abortRef.current = new AbortController();

    try {
      // 1) Create server order (backend will lookup gym_plans for price/duration)
      const orderRes = await PaymentsAPI.createGymOrder(opts.gymId, opts.planId, opts.startOn);
      const { razorpay_order_id, razorpay_key_id, amount_minor, currency, display_title } = orderRes;

      // 2) Launch Razorpay Checkout
      const checkoutOptions = {
        key: razorpay_key_id,          // your public key id
        order_id: razorpay_order_id,   // REQUIRED
        amount: amount_minor,          // paise (backend converts from gym_plans.amount)
        currency: currency || "INR",
        name: "FittBot",
        description: opts.description || display_title || "Gym membership",
        image: opts.image,             // optional logo
        prefill: {
          name: opts.prefill?.name,
          email: opts.prefill?.email,
          contact: opts.prefill?.contact,
        },
        theme: { color: opts.themeColor || "#111827" },
        retry: { enabled: true, max_count: 1 },
        notes: { gymId: String(opts.gymId), planId: String(opts.planId) },
      };

      const rzpResp = await new Promise((resolve, reject) => {
        RazorpayCheckout.open(checkoutOptions).then(resolve).catch(reject);
      });

      const { razorpay_payment_id, razorpay_signature } = rzpResp || {};
      if (!razorpay_payment_id || !razorpay_signature) {
        return { ok: false, error: "Payment response incomplete." };
      }

      // 3) Verify on backend (creates membership, sets activation based on payment time + duration)
      let verifyRes = await PaymentsAPI.verifyGymPayment(
        razorpay_payment_id,
        razorpay_order_id,
        razorpay_signature
      );

      // 4) Retry loop (honor server backoff)
      let attempts = 0;
      while (verifyRes?.verified && !verifyRes?.captured && attempts < 5) {
        const delayMs = Math.min(8000, Number(verifyRes?.retryAfterMs || 3000));
        await new Promise(r => setTimeout(r, delayMs));
        verifyRes = await PaymentsAPI.verifyGymPayment(
          razorpay_payment_id,
          razorpay_order_id,
          razorpay_signature
        );
        attempts++;
      }

      if (verifyRes?.verified && verifyRes?.captured) {
        return { ok: true, data: verifyRes };
      }

      if (verifyRes?.status === "failed" || verifyRes?.status === "refunded") {
        return { ok: false, error: `Payment ${verifyRes.status}` };
      }

      return { ok: false, error: "Payment verification pending. Please check later." };
    } catch (err) {
      const msg = err?.message || "Payment failed";
      return { ok: false, error: msg, code: err?.status };
    } finally {
      setBusy(false);
    }
  }, [busy]);

  return { start, cancel, busy };
}