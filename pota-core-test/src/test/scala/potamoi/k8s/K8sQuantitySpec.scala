package potamoi.k8s

import potamoi.testkit.STSpec

class K8sQuantitySpec extends STSpec {

  import QuantityUnit._

  "convert string to QuantityUnit" in {
    QuantityUnit.resolve("100m") shouldBe K8sQuantity(100, m)
    QuantityUnit.resolve("233Ki") shouldBe K8sQuantity(233, Ki)
    QuantityUnit.resolve("233Gi") shouldBe K8sQuantity(233, Gi)
    QuantityUnit.resolve("233") shouldBe K8sQuantity(233, k)
  }

  "convert value by unit" in {
    K8sQuantity(1000, m).to(u) shouldBe 1000 * 1000
    K8sQuantity(1000, u).to(m) shouldBe 1
    K8sQuantity(1, Gi).to(Ki) shouldBe 1024 * 1024
    K8sQuantity(1024, Ki).to(Mi) shouldBe 1
    K8sQuantity(1024 * 1024, Ki).to(Gi) shouldBe 1
    K8sQuantity(1000, k).to(k) shouldBe 1000
    K8sQuantity(1000, Ki).to(Ki) shouldBe 1000

    K8sQuantity(1.024, Ki).to(k) shouldBe 1
    K8sQuantity(1.024, Gi).to(G) shouldBe 1
    K8sQuantity(1.024, Mi).to(M) shouldBe 1
    K8sQuantity(1.024, Ki).to(m) shouldBe 1000
    K8sQuantity(1.024, Gi).to(k) shouldBe 1000 * 1000

    K8sQuantity(1, k).to(Ki) shouldBe 1.024
    K8sQuantity(1, G).to(Gi) shouldBe 1.024
    K8sQuantity(1, M).to(Mi) shouldBe 1.024
    K8sQuantity(1.5, k).to(Mi) shouldBe 1536
  }

}
