// GymMembershipPayButton.jsx
import React, { useState } from "react";
import { TouchableOpacity, Text, View, ActivityIndicator, StyleSheet, Modal } from "react-native";
import dayjs from "dayjs";
import { useGymMembershipCheckout } from "./useGymMembershipCheckout";

export default function GymMembershipPayButton(props) {
  const { gymId, planId, startOn, label = "Buy membership", prefill } = props;
  const { start, busy } = useGymMembershipCheckout();
  const [result, setResult] = useState(null);
  const [open, setOpen] = useState(false);

  async function onPress() {
    const res = await start({
      gymId,
      planId,
      startOn, // optional - defaults to payment time if not provided
      prefill,
      themeColor: "#0ea5e9",
      description: "Secure checkout via Razorpay",
    });
    setResult(res);
    setOpen(true);
  }

  function close() {
    setOpen(false);
    setResult(null);
  }

  const btnDisabled = busy;

  return (
    <>
      <TouchableOpacity
        disabled={btnDisabled}
        onPress={onPress}
        style={[styles.btn, btnDisabled && styles.btnDisabled]}
        activeOpacity={0.8}
      >
        {busy ? <ActivityIndicator /> : <Text style={styles.btnText}>{label}</Text>}
      </TouchableOpacity>

      <Modal visible={open} transparent animationType="slide" onRequestClose={close}>
        <View style={styles.sheetBackdrop}>
          <View style={styles.sheet}>
            {result?.ok ? (
              <>
                <Text style={styles.title}>Payment successful 🎉</Text>
                <Text style={styles.caption}>Your membership is now active.</Text>
                <View style={styles.row}>
                  <Text style={styles.k}>Active from</Text>
                  <Text style={styles.v}>
                    {result?.data?.active_from ? dayjs(result.data.active_from).format("DD MMM YYYY") : "—"}
                  </Text>
                </View>
                <View style={styles.row}>
                  <Text style={styles.k}>Active until</Text>
                  <Text style={styles.v}>
                    {result?.data?.active_until ? dayjs(result.data.active_until).format("DD MMM YYYY") : "—"}
                  </Text>
                </View>
                <View style={styles.row}>
                  <Text style={styles.k}>Order ID</Text>
                  <Text style={styles.v}>{result?.data?.order_id || "—"}</Text>
                </View>
                <View style={styles.row}>
                  <Text style={styles.k}>Entitlement ID</Text>
                  <Text style={styles.v}>{result?.data?.entitlement_id || "—"}</Text>
                </View>
              </>
            ) : (
              <>
                <Text style={styles.title}>Payment status</Text>
                <Text style={[styles.caption, { color: "#b91c1c" }]}>
                  {result?.error || "Verification pending. You'll see the pass soon."}
                </Text>
              </>
            )}
            <TouchableOpacity style={[styles.btn, { marginTop: 16 }]} onPress={close}>
              <Text style={styles.btnText}>Close</Text>
            </TouchableOpacity>
          </View>
        </View>
      </Modal>
    </>
  );
}

const styles = StyleSheet.create({
  btn: {
    backgroundColor: "#0ea5e9",
    paddingVertical: 12,
    paddingHorizontal: 16,
    borderRadius: 999,
    alignItems: "center",
    justifyContent: "center",
    shadowColor: "#000",
    shadowOpacity: 0.15,
    shadowRadius: 4,
    shadowOffset: { width: 0, height: 2 },
    elevation: 2,
  },
  btnDisabled: { opacity: 0.6 },
  btnText: { color: "#fff", fontSize: 16, fontWeight: "600" },
  sheetBackdrop: { flex: 1, backgroundColor: "rgba(0,0,0,0.4)", justifyContent: "flex-end" },
  sheet: {
    backgroundColor: "#111827",
    padding: 20,
    borderTopLeftRadius: 16,
    borderTopRightRadius: 16,
  },
  title: { color: "#fff", fontSize: 18, fontWeight: "700", marginBottom: 6 },
  caption: { color: "#9ca3af", fontSize: 14, marginBottom: 12 },
  row: { flexDirection: "row", justifyContent: "space-between", marginTop: 6 },
  k: { color: "#9ca3af" },
  v: { color: "#fff", fontWeight: "600" },
});