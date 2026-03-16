/**
 * Drop-in snippet for Razorpay verify + polling flow
 * Copy this block into your screen/component.
 * It only changes the verify step to handle { captured: true } immediately;
 * everything else is unchanged.
 */

const [razorpayPollingAttempt, setRazorpayPollingAttempt] = useState(0);
const timerRef = useRef(null);
const clearTimer = () => {
  if (timerRef.current) clearInterval(timerRef.current);
  timerRef.current = null;
};

useEffect(() => clearTimer, []);

const pollPremium = async (attempt = 1) => {
  try {
    setProcessingStep(`Verifying payment`);
    const clientId = await AsyncStorage.getItem("client_id");
    if (!clientId) throw new Error("Client ID not found");
    const { data } = await axios.get(
      `${API_BASE}/payments/user/${clientId}/premium-status`,
      {
        headers: {
          "ngrok-skip-browser-warning": "true",
        },
      }
    );
    return data?.has_premium;
  } catch (error) {
    console.error(`Premium status check attempt ${attempt} failed:`, error);
    return false;
  }
};

const startPolling = () => {
  let attemptCount = 0;
  setProcessingStep("Verifying with backend...");

  timerRef.current = setInterval(async () => {
    attemptCount++;
    setRazorpayPollingAttempt(attemptCount);

    if (await pollPremium(attemptCount)) {
      setBackendVerified(true);
      showToast({
        type: "success",
        title: "Success!",
        desc: "You have successfully subscribed to Premium Plan!",
      });

      // Wait a moment to show success, then navigate
      clearTimer();
      setTimeout(() => {
        setIsProcessing(false);
        router.push("/client/home");
      }, 2000);
    } else if (attemptCount >= 10) {
      // Stop polling after 10 attempts (25 seconds)
      clearTimer();
      setIsProcessing(false);
      showToast({
        type: "error",
        title: "Verification Timeout",
        desc: "Payment verification is taking longer than expected. Please check your account or contact support.",
      });
    }
  }, 2500);
};

const handleRazorpayPayment = useCallback(async () => {
  if (isProcessing) return;
  setIsProcessing(true);
  setProcessingStep("Initializing...");

  try {
    // 1) Get client ID and check prerequisites
    const clientId = await AsyncStorage.getItem("client_id");
    if (!clientId) throw new Error("Client ID not found");

    // 2) Check current premium status
    setProcessingStep("Checking account status...");
    const { data } = await axios.get(
      `${API_BASE}/payments/user/${clientId}/premium-status`,
      {
        headers: {
          "ngrok-skip-browser-warning": "true",
        },
      }
    );

    if (data?.has_premium) {
      showToast({
        type: "info",
        title: "Active Premium User",
        desc: "You already have an active premium subscription!",
      });
      setIsProcessing(false);
      return;
    }

    // 3) Create subscription on server
    setProcessingStep("Creating subscription...");
    const prefill = {
      email: "martinraju53@gmail.com",
      contact: "8667458723",
      name: "Martin raju",
    };

    const { data: subscriptionData } = await axios.post(
      `${API_BASE}/payments/razorpay/subscriptions/create`,
      {
        user_id: clientId,
        plan_sku: planSku,
      }
    );
    console.log("Subscription data:", subscriptionData);

    // 4) Open Razorpay checkout for the created subscription
    setProcessingStep("Setting up payment...");
    const options = {
      key: subscriptionData.razorpay_key_id,
      subscription_id: subscriptionData.subscription_id,
      name: "Fittbot",
      description: subscriptionData.display_title || "Premium Subscription",
      prefill: prefill || {},
      theme: { color: "#FF5757" },
    };

    setProcessingStep("Processing payment...");
    const result = await RazorpayCheckout.open(options);

    // 5) Verify the checkout signature with server (updated)
    setProcessingStep("Verifying payment...");
    const { data: verify } = await axios.post(
      `${API_BASE}/payments/razorpay/subscriptions/verify`,
      {
        razorpay_payment_id: result.razorpay_payment_id,
        razorpay_subscription_id: result.razorpay_subscription_id,
        razorpay_signature: result.razorpay_signature,
      }
    );

    if (verify?.captured === true) {
      // Immediate success (payment captured and recorded server-side)
      setBackendVerified(true);
      showToast({
        type: "success",
        title: "Success!",
        desc: "You have successfully subscribed to Premium Plan!",
      });
      clearTimer();
      setTimeout(() => {
        setIsProcessing(false);
        router.push("/client/home");
      }, 2000);
    } else if (verify?.verified === false) {
      // Payment failed/refunded
      clearTimer();
      setIsProcessing(false);
      showToast({
        type: "error",
        title: "Payment Failed",
        desc: "Payment was not captured. Please try again.",
      });
    } else {
      // 6) Await webhook to flip premium, then poll status
      startPolling();
    }
  } catch (err) {
    clearTimer();
    setIsProcessing(false);
    if (err?.code !== 2) {
      Alert.alert(
        "Payment Error",
        err?.description || "Payment failed or cancelled."
      );
    }
  }
}, [API_BASE, planSku, isProcessing]);

