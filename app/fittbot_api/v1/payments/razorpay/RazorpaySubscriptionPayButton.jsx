import React, { useCallback, useState, useRef, useEffect } from "react";
import { View, Text, Pressable, ActivityIndicator, Alert, StyleSheet } from "react-native";
import AsyncStorage from "@react-native-async-storage/async-storage";
import RazorpayCheckout from "react-native-razorpay";
import axios from "axios";

/**
 * RazorpaySubscriptionPayButton
 *
 * Works with server endpoints:
 *  - POST  /payments/razorpay/subscriptions/create
 *  - POST  /payments/razorpay/subscriptions/verify  -> returns { captured: boolean }
 *  - GET   /payments/user/{clientId}/premium-status
 */
const RazorpaySubscriptionPayButton = ({
  apiBase,
  clientId: clientIdProp,
  planSku,
  prefill,
  themeColor = "#111827",
  onPremiumActive,
  showToast, // optional: ({type,title,desc}) => void
  additionalHeaders = {},
}) => {
  const [loading, setLoading] = useState(false);
  const [label, setLabel] = useState("Pay & Subscribe");
  const [attempt, setAttempt] = useState(0);
  const timerRef = useRef(null);

  const clearTimer = () => {
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = null;
  };

  useEffect(() => clearTimer, []);

  const pollPremium = async (attemptNo = 0) => {
    try {
      const cid = clientIdProp || (await AsyncStorage.getItem("client_id"));
      if (!cid) throw new Error("Client ID not found");
      const { data } = await axios.get(
        `${apiBase}/payments/user/${cid}/premium-status`,
        { headers: { "ngrok-skip-browser-warning": "true", ...additionalHeaders } }
      );
      return data?.has_premium;
    } catch (err) {
      console.log(`[RZP] premium-status attempt ${attemptNo} error`, err?.message || err);
      return false;
    }
  };

  const startPolling = () => {
    let localAttempt = 0;
    setLabel("Verifying with backend…");
    timerRef.current = setInterval(async () => {
      localAttempt += 1;
      setAttempt(localAttempt);
      const ok = await pollPremium(localAttempt);
      if (ok) {
        clearTimer();
        setLoading(false);
        setLabel("Subscribed ✅");
        showToast?.({ type: "success", title: "Success!", desc: "Premium activated" });
        onPremiumActive?.();
      } else if (localAttempt >= 10) {
        clearTimer();
        setLoading(false);
        setLabel("Verification Timeout");
        showToast?.({
          type: "error",
          title: "Verification Timeout",
          desc: "Payment verification is taking longer than expected. Please check your account or try again.",
        });
      }
    }, 2500);
  };

  const handlePay = useCallback(async () => {
    if (loading) return;
    setLoading(true);
    setLabel("Initializing…");

    try {
      // 1) Resolve client id
      const cid = clientIdProp || (await AsyncStorage.getItem("client_id"));
      if (!cid) throw new Error("Client ID not found");

      // 2) Create subscription
      const { data } = await axios.post(
        `${apiBase}/payments/razorpay/subscriptions/create`,
        { user_id: cid, plan_sku: planSku },
        { headers: { "ngrok-skip-browser-warning": "true", ...additionalHeaders } }
      );

      // 3) Open Razorpay checkout
      const options = {
        key: data.razorpay_key_id,
        subscription_id: data.subscription_id,
        name: "Fittbot",
        description: data.display_title || "Premium Subscription",
        prefill: prefill || {},
        theme: { color: themeColor },
      };
      const result = await RazorpayCheckout.open(options);

      // 4) Verify with backend (may return captured immediately)
      setLabel("Verifying payment…");
      const { data: verify } = await axios.post(
        `${apiBase}/payments/razorpay/subscriptions/verify`,
        {
          razorpay_payment_id: result.razorpay_payment_id,
          razorpay_subscription_id: result.razorpay_subscription_id,
          razorpay_signature: result.razorpay_signature,
        },
        { headers: { "ngrok-skip-browser-warning": "true", ...additionalHeaders } }
      );

      if (verify?.captured === true) {
        setLabel("Subscribed ✅");
        setLoading(false);
        showToast?.({ type: "success", title: "Success!", desc: "Payment captured and recorded" });
        onPremiumActive?.();
        return;
      }

      // 5) Otherwise wait for webhook and poll premium gate
      setLabel("Activating…");
      startPolling();
    } catch (err) {
      clearTimer();
      setLoading(false);
      setLabel("Pay & Subscribe");
      if (err?.code !== 2) {
        Alert.alert("Payment Error", err?.description || "Payment failed or cancelled.");
      }
    }
  }, [apiBase, clientIdProp, planSku, prefill, themeColor, loading, additionalHeaders, onPremiumActive, showToast]);

  return (
    <View style={styles.container}>
      <Pressable style={[styles.btn, loading && styles.btnDisabled]} onPress={handlePay} disabled={loading}>
        {loading ? <ActivityIndicator color="#fff" /> : <Text style={styles.btnText}>{label}</Text>}
      </Pressable>
      {loading ? <Text style={styles.subText}>Attempt: {attempt}</Text> : null}
    </View>
  );
};

const styles = StyleSheet.create({
  container: { width: "100%", alignItems: "center" },
  btn: { backgroundColor: "#111827", paddingVertical: 14, paddingHorizontal: 18, borderRadius: 10, minWidth: 220, alignItems: "center" },
  btnDisabled: { opacity: 0.6 },
  btnText: { color: "#fff", fontWeight: "600" },
  subText: { marginTop: 8, color: "#4B5563", fontSize: 12 },
});

export default RazorpaySubscriptionPayButton;

