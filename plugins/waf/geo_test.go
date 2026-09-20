package waf

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

// writeFixtureCountry writes a small GeoLite2-style Country database into dir:
// 8.8.8.0/24 -> country US, 8.8.4.0/24 -> registered_country GB only.
func writeFixtureCountry(t *testing.T, dir string) {
	t.Helper()
	w, err := mmdbwriter.New(mmdbwriter.Options{DatabaseType: "GeoLite2-Country", RecordSize: 24})
	if err != nil {
		t.Fatalf("mmdbwriter.New: %v", err)
	}
	netUS := &net.IPNet{IP: net.ParseIP("8.8.8.0").To4(), Mask: net.CIDRMask(24, 32)}
	if err := w.Insert(netUS, mmdbtype.Map{
		"country": mmdbtype.Map{"iso_code": mmdbtype.String("US")},
	}); err != nil {
		t.Fatalf("Insert country: %v", err)
	}
	netGB := &net.IPNet{IP: net.ParseIP("8.8.4.0").To4(), Mask: net.CIDRMask(24, 32)}
	if err := w.Insert(netGB, mmdbtype.Map{
		"registered_country": mmdbtype.Map{"iso_code": mmdbtype.String("GB")},
	}); err != nil {
		t.Fatalf("Insert registered_country: %v", err)
	}
	fh, err := os.Create(filepath.Join(dir, "GeoLite2-Country.mmdb"))
	if err != nil {
		t.Fatalf("create country file: %v", err)
	}
	defer fh.Close()
	if _, err := w.WriteTo(fh); err != nil {
		t.Fatalf("write country db: %v", err)
	}
}

// writeFixtureASN writes a small GeoLite2-style ASN database into dir:
// 8.8.8.0/24 -> ASN 15169 "GOOGLE".
func writeFixtureASN(t *testing.T, dir string) {
	t.Helper()
	w, err := mmdbwriter.New(mmdbwriter.Options{DatabaseType: "GeoLite2-ASN", RecordSize: 24})
	if err != nil {
		t.Fatalf("mmdbwriter.New: %v", err)
	}
	netUS := &net.IPNet{IP: net.ParseIP("8.8.8.0").To4(), Mask: net.CIDRMask(24, 32)}
	if err := w.Insert(netUS, mmdbtype.Map{
		"autonomous_system_number":       mmdbtype.Uint32(15169),
		"autonomous_system_organization": mmdbtype.String("GOOGLE"),
	}); err != nil {
		t.Fatalf("Insert asn: %v", err)
	}
	fh, err := os.Create(filepath.Join(dir, "GeoLite2-ASN.mmdb"))
	if err != nil {
		t.Fatalf("create asn file: %v", err)
	}
	defer fh.Close()
	if _, err := w.WriteTo(fh); err != nil {
		t.Fatalf("write asn db: %v", err)
	}
}

// writeFixtureCity writes a small GeoLite2-style City database into dir:
// 8.8.8.0/24 -> city "Mountain View" (English name).
func writeFixtureCity(t *testing.T, dir string) {
	t.Helper()
	w, err := mmdbwriter.New(mmdbwriter.Options{DatabaseType: "GeoLite2-City", RecordSize: 24})
	if err != nil {
		t.Fatalf("mmdbwriter.New: %v", err)
	}
	netUS := &net.IPNet{IP: net.ParseIP("8.8.8.0").To4(), Mask: net.CIDRMask(24, 32)}
	if err := w.Insert(netUS, mmdbtype.Map{
		"city": mmdbtype.Map{"names": mmdbtype.Map{
			"en": mmdbtype.String("Mountain View"),
		}},
	}); err != nil {
		t.Fatalf("Insert city: %v", err)
	}
	fh, err := os.Create(filepath.Join(dir, "GeoLite2-City.mmdb"))
	if err != nil {
		t.Fatalf("create city file: %v", err)
	}
	defer fh.Close()
	if _, err := w.WriteTo(fh); err != nil {
		t.Fatalf("write city db: %v", err)
	}
}

// writeFixtureDir creates a directory with both Country and ASN fixtures.
func writeFixtureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFixtureCountry(t, dir)
	writeFixtureASN(t, dir)
	return dir
}

// writeFixtureDirAll creates a directory with Country, ASN and City fixtures.
func writeFixtureDirAll(t *testing.T) string {
	t.Helper()
	dir := writeFixtureDir(t)
	writeFixtureCity(t, dir)
	return dir
}

func TestOpenGeodbEmptyDir(t *testing.T) {
	g, err := openGeodb("")
	if err != nil {
		t.Fatalf("openGeodb(\"\") error: %v", err)
	}
	if g.country != nil || g.asn != nil || g.city != nil {
		t.Fatalf("expected all readers nil, got country=%v asn=%v city=%v", g.country, g.asn, g.city)
	}
}

func TestOpenGeodbMissingCountry(t *testing.T) {
	dir := t.TempDir()
	if _, err := openGeodb(dir); err == nil {
		t.Fatal("expected error when Country db is missing")
	}
}

func TestOpenGeodbFixtureDir(t *testing.T) {
	dir := writeFixtureDir(t)
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb error: %v", err)
	}
	if g.country == nil {
		t.Fatal("expected country reader")
	}
	if g.asn == nil {
		t.Fatal("expected asn reader")
	}
	if g.city != nil {
		t.Fatal("expected city reader to be nil (no city fixture)")
	}
}

func TestOpenGeodbWithCity(t *testing.T) {
	dir := writeFixtureDirAll(t)
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb error: %v", err)
	}
	if g.city == nil {
		t.Fatal("expected city reader")
	}
}

func TestOpenGeodbMissingASN(t *testing.T) {
	dir := t.TempDir()
	writeFixtureCountry(t, dir)
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("missing ASN db should degrade, got error: %v", err)
	}
	if g.asn != nil {
		t.Fatal("expected asn reader nil when file missing")
	}
}

func TestOpenGeodbBadCity(t *testing.T) {
	dir := writeFixtureDir(t)
	if err := os.WriteFile(filepath.Join(dir, "GeoLite2-City.mmdb"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("write junk city: %v", err)
	}
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("bad city db should degrade, got error: %v", err)
	}
	if g.city != nil {
		t.Fatal("expected city reader nil for corrupt file")
	}
}

func TestGeodbLookup(t *testing.T) {
	dir := writeFixtureDir(t)
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	country, asn, org, city := g.lookup(net.ParseIP("8.8.8.8"))
	if country != "US" || asn != 15169 || org != "GOOGLE" || city != "" {
		t.Fatalf("lookup 8.8.8.8 = (%q, %d, %q, %q)", country, asn, org, city)
	}
	// registered_country fallback + no ASN record.
	country, asn, org, city = g.lookup(net.ParseIP("8.8.4.4"))
	if country != "GB" || asn != 0 || org != "" || city != "" {
		t.Fatalf("lookup 8.8.4.4 = (%q, %d, %q, %q)", country, asn, org, city)
	}
}

func TestGeodbLookupCity(t *testing.T) {
	dir := writeFixtureDirAll(t)
	g, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	country, asn, org, city := g.lookup(net.ParseIP("8.8.8.8"))
	if country != "US" || asn != 15169 || org != "GOOGLE" || city != "Mountain View" {
		t.Fatalf("lookup 8.8.8.8 = (%q, %d, %q, %q)", country, asn, org, city)
	}
	// No city record for this network -> empty city, no error.
	_, _, _, city = g.lookup(net.ParseIP("8.8.4.4"))
	if city != "" {
		t.Fatalf("lookup 8.8.4.4 city = %q", city)
	}
}

func TestGeodbLookupNoReaders(t *testing.T) {
	g, err := openGeodb("")
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	country, asn, org, city := g.lookup(net.ParseIP("8.8.8.8"))
	if country != "" || asn != 0 || org != "" || city != "" {
		t.Fatalf("lookup with no readers = (%q, %d, %q, %q)", country, asn, org, city)
	}
}

func TestGeodbClose(t *testing.T) {
	g, err := openGeodb("")
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	g.close()

	dir := writeFixtureDir(t)
	g, err = openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	g.close()

	dir = writeFixtureDirAll(t)
	g, err = openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	g.close()
}
