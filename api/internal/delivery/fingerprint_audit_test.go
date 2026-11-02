package delivery_test

import (
	"math"
	"testing"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/google/uuid"
)

// -----------------------------------------------------------------------------
// AUDITORIA PONTO 4: FINGERPRINT SEMÂNTICO
// -----------------------------------------------------------------------------

// 1. Precisão de coordenadas em 6 casas decimais (11 cm) e limites de arredondamento
func TestAudit_Fingerprint_CoordinatePrecisionBoundaries(t *testing.T) {
	aggID := uuid.New()

	baseLat := -23.550500
	baseLng := -46.633300

	evBase := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-base",
		DeliveryID: aggID,
		Seq:        1,
		Lat:        baseLat,
		Lng:        baseLng,
	}
	hashBase, err := delivery.ComputeFingerprint(evBase)
	if err != nil {
		t.Fatalf("failed to compute base fingerprint: %v", err)
	}

	// Cenário A: Jitter infinitesimal de GPS (< 5 cm / 0.0000004 graus)
	// Deve ser absorvido pelo arredondamento de %.6f, mantendo o MESMO hash semântico
	evSubCentimeter := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-subcm",
		DeliveryID: aggID,
		Seq:        1,
		Lat:        baseLat + 0.0000003, // ~3.3 cm de diferença
		Lng:        baseLng - 0.0000002, // ~2.2 cm de diferença
	}
	hashSubCentimeter, err := delivery.ComputeFingerprint(evSubCentimeter)
	if err != nil {
		t.Fatalf("failed to compute subcm fingerprint: %v", err)
	}

	if hashBase != hashSubCentimeter {
		t.Fatalf("esperava idempotência para jitter < 5cm: hashBase=%s, hashSubCm=%s", hashBase, hashSubCentimeter)
	}
	t.Logf("CONFIRMADO: Jitter sub-5cm absorvido com sucesso por %%.6f (mesmo hash: %s)", hashBase)

	// Cenário B: Deslocamento mensurável (> 15 cm / 0.000002 graus)
	// Deve gerar fingerprint DIFERENTE
	evDisplaced := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-disp",
		DeliveryID: aggID,
		Seq:        1,
		Lat:        baseLat + 0.000002, // ~22 cm
		Lng:        baseLng,
	}
	hashDisplaced, err := delivery.ComputeFingerprint(evDisplaced)
	if err != nil {
		t.Fatalf("failed to compute displaced fingerprint: %v", err)
	}

	if hashBase == hashDisplaced {
		t.Fatalf("falha: deslocamento mensurável de 22cm não alterou o hash!")
	}
	t.Logf("CONFIRMADO: Deslocamento de 22cm detectado (hashBase=%s vs hashDisplaced=%s)", hashBase, hashDisplaced)
}

// 2. Comportamento com IEEE 754 Negative Zero (-0.0 vs +0.0)
func TestAudit_Fingerprint_NegativeZero(t *testing.T) {
	aggID := uuid.New()

	evPosZero := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-pos-0",
		DeliveryID: aggID,
		Seq:        1,
		Lat:        0.0,
		Lng:        0.0,
	}
	hPos, err := delivery.ComputeFingerprint(evPosZero)
	if err != nil {
		t.Fatalf("failed to compute pos zero hash: %v", err)
	}

	negZero := math.Copysign(0.0, -1.0)
	evNegZero := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-neg-0",
		DeliveryID: aggID,
		Seq:        1,
		Lat:        negZero,
		Lng:        0.0,
	}
	hNeg, err := delivery.ComputeFingerprint(evNegZero)
	if err != nil {
		t.Fatalf("failed to compute neg zero hash: %v", err)
	}

	if hPos != hNeg {
		t.Fatalf("falha: +0.0 (%s) e -0.0 (%s) geraram fingerprints diferentes!", hPos, hNeg)
	}
	t.Logf("CONFIRMADO: EventDriverEnRouteToDropoff: +0.0 e -0.0 geram fingerprints idênticos (%s)", hPos)

	// B. DeliveryCreated com -0.0 em pickup e dropoff
	evCreatedPos := delivery.EventDeliveryCreated{
		ID:         "ev-created-pos",
		DeliveryID: aggID,
		Seq:        1,
		PickupLat:  0.0,
		PickupLng:  0.0,
		DropoffLat: 0.0,
		DropoffLng: 0.0,
	}
	hCreatedPos, _ := delivery.ComputeFingerprint(evCreatedPos)

	evCreatedNeg := delivery.EventDeliveryCreated{
		ID:         "ev-created-neg",
		DeliveryID: aggID,
		Seq:        1,
		PickupLat:  negZero,
		PickupLng:  negZero,
		DropoffLat: negZero,
		DropoffLng: negZero,
	}
	hCreatedNeg, _ := delivery.ComputeFingerprint(evCreatedNeg)

	if hCreatedPos != hCreatedNeg {
		t.Fatalf("falha no DeliveryCreated: +0.0 (%s) != -0.0 (%s)", hCreatedPos, hCreatedNeg)
	}
	t.Logf("CONFIRMADO: EventDeliveryCreated: +0.0 e -0.0 geram fingerprints idênticos (%s)", hCreatedPos)

	// C. DeliveryRouteAssigned com -0.0 em Points
	evRoutePos := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-route-pos",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			Points: []delivery.CoordinatePayload{{Lat: 0.0, Lng: 0.0}},
		},
	}
	hRoutePos, _ := delivery.ComputeFingerprint(evRoutePos)

	evRouteNeg := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-route-neg",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			Points: []delivery.CoordinatePayload{{Lat: negZero, Lng: negZero}},
		},
	}
	hRouteNeg, _ := delivery.ComputeFingerprint(evRouteNeg)

	if hRoutePos != hRouteNeg {
		t.Fatalf("falha no DeliveryRouteAssigned: +0.0 (%s) != -0.0 (%s)", hRoutePos, hRouteNeg)
	}
	t.Logf("CONFIRMADO: EventDeliveryRouteAssigned: +0.0 e -0.0 geram fingerprints idênticos (%s)", hRoutePos)
}

// 3. Comportamento com strings vazias e campos opcionais
func TestAudit_Fingerprint_EmptyStringsAndOptionalFields(t *testing.T) {
	aggID := uuid.New()

	// A. DriverID vazio em EventDeliveryCreated
	evEmptyDriver := delivery.EventDeliveryCreated{
		ID:         "ev-empty-driver",
		DeliveryID: aggID,
		DriverID:   "",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	hEmpty, err := delivery.ComputeFingerprint(evEmptyDriver)
	if err != nil || len(hEmpty) != 64 {
		t.Fatalf("erro com DriverID vazio: %v", err)
	}
	t.Logf("DriverID vazio produz hash válido: %s", hEmpty)

	// B. Route com Points nil vs Points slice vazio []
	evNilPoints := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-nil-pts",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			RouteID:  "r1",
			Revision: 1,
			Leg:      "pickup",
			Points:   nil,
		},
	}
	hNilPts, err := delivery.ComputeFingerprint(evNilPoints)
	if err != nil {
		t.Fatalf("erro com nil points: %v", err)
	}

	evEmptySlicePts := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-empty-slice-pts",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			RouteID:  "r1",
			Revision: 1,
			Leg:      "pickup",
			Points:   []delivery.CoordinatePayload{},
		},
	}
	hEmptySlicePts, err := delivery.ComputeFingerprint(evEmptySlicePts)
	if err != nil {
		t.Fatalf("erro com empty slice points: %v", err)
	}

	if hNilPts != hEmptySlicePts {
		t.Fatalf("inconsistência: points=nil (%s) != points=[] (%s)", hNilPts, hEmptySlicePts)
	}
	t.Logf("CONFIRMADO: points=nil e points=[] produzem o mesmo hash semântico (%s)", hNilPts)
}
