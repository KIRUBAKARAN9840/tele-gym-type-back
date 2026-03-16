// CheckoutScreen.jsx (example usage)
import React from "react";
import { View, Text, StyleSheet } from "react-native";
import GymMembershipPayButton from "./GymMembershipPayButton";

export default function CheckoutScreen() {
  // These would come from your app navigation/props
  const gymId = 101;     // from route / product list
  const planId = 3;      // e.g., plan ID for "1 month" (backend will lookup gym_plans table)
  const startOn = undefined; // optional - if not provided, membership starts from payment time

  return (
    <View style={styles.wrap}>
      <Text style={styles.title}>Elite Fitness — 1 Month</Text>
      <Text style={styles.subtitle}>Backend will lookup price & duration from gym_plans table</Text>

      <GymMembershipPayButton
        gymId={gymId}
        planId={planId}
        startOn={startOn} // optional
        prefill={{
          name: "John Doe",
          email: "john@example.com",
          contact: "9999999999"
        }}
        label="Pay for Membership"
      />
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { flex: 1, backgroundColor: "#0b1220", padding: 16, justifyContent: "center" },
  title: { color: "#fff", fontSize: 22, fontWeight: "800", marginBottom: 8 },
  subtitle: { color: "#9ca3af", marginBottom: 24 },
});