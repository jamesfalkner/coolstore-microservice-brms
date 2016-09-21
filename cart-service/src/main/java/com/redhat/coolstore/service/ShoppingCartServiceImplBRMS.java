package com.redhat.coolstore.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.coolstore.model.Product;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;

import com.redhat.coolstore.PromoEvent;
import com.redhat.coolstore.model.Promotion;
import com.redhat.coolstore.model.ShoppingCart;
import com.redhat.coolstore.model.ShoppingCartItem;
import com.redhat.coolstore.util.BRMSUtil;

@ApplicationScoped
public class ShoppingCartServiceImplBRMS implements ShoppingCartService, Serializable {

    private static final long serialVersionUID = 682195219434330759L;

    @Inject
    private BRMSUtil brmsUtil;

    @Inject
    private PromoService promoService;

    private Map<String, ShoppingCart> cartDB = new HashMap<>();

    private Map<String, Product> productMap = new HashMap<>();

    public ShoppingCartServiceImplBRMS() {

    }

    @Override
    public ShoppingCart getShoppingCart(String cartId) {
        if (!cartDB.containsKey(cartId)) {
            ShoppingCart c = new ShoppingCart();
            cartDB.put(cartId, c);
            return c;
        } else {
            ShoppingCart sc = cartDB.get(cartId);
            priceShoppingCart(sc);
            cartDB.put(cartId, sc);
            return cartDB.get(cartId);
        }
    }

    private void initShoppingCartForPricing(ShoppingCart sc) {

        sc.setCartItemTotal(0);
        sc.setCartItemPromoSavings(0);
        sc.setShippingTotal(0);
        sc.setShippingPromoSavings(0);
        sc.setCartTotal(0);

        for (ShoppingCartItem sci : sc.getShoppingCartItemList()) {

            Product p = getProduct(sci.getProduct().getItemId());

            //if product exist, create new product to reset price
            if ( p != null ) {

                sci.setProduct(new Product(p.getItemId(), p.getName(), p.getDesc(), p.getPrice()));
                sci.setPrice(p.getPrice());
            }

            sci.setPromoSavings(0);

        }


    }

    @Override
    public Product getProduct(String itemId) {
        if (!productMap.containsKey(itemId)) {

            CatalogService cat = Feign.builder()
                    .decoder(new JacksonDecoder())
                    .target(CatalogService.class, "http://catalog-service:8080");

            // Fetch and cache products. TODO: Cache should expire at some point!
            List<Product> products = cat.products();
            productMap = products.stream().collect(Collectors.toMap(Product::getItemId, Function.identity()));
        }

        return productMap.get(itemId);
    }

    @Override
    public void priceShoppingCart(ShoppingCart sc) {

        System.out.println("USING BRMS FOR PRICING");

        initShoppingCartForPricing(sc);

        if ( sc != null ) {

            com.redhat.coolstore.ShoppingCart factShoppingCart = new com.redhat.coolstore.ShoppingCart();

            factShoppingCart.setCartItemPromoSavings(0d);
            factShoppingCart.setCartItemTotal(0d);
            factShoppingCart.setCartTotal(0d);
            factShoppingCart.setShippingPromoSavings(0d);
            factShoppingCart.setShippingTotal(0d);

            KieSession ksession = null;

            try {

                //if at least one shopping cart item exist
                if ( sc.getShoppingCartItemList().size() > 0 ) {

                    ksession = brmsUtil.getStatefulSession();

                    EntryPoint promoStream = ksession.getEntryPoint("Promo Stream");

                    for (Promotion promo : promoService.getPromotions()) {

                        PromoEvent pv = new PromoEvent(promo.getItemId(), promo.getPercentOff());

                        promoStream.insert(pv);

                    }

                    ksession.insert(factShoppingCart);

                    for (ShoppingCartItem sci : sc.getShoppingCartItemList()) {

                        com.redhat.coolstore.ShoppingCartItem factShoppingCartItem = new com.redhat.coolstore.ShoppingCartItem();
                        factShoppingCartItem.setItemId(sci.getProduct().getItemId());
                        factShoppingCartItem.setName(sci.getProduct().getName());
                        factShoppingCartItem.setPrice(sci.getProduct().getPrice());
                        factShoppingCartItem.setQuantity(sci.getQuantity());
                        factShoppingCartItem.setShoppingCart(factShoppingCart);
                        factShoppingCartItem.setPromoSavings(0d);

                        ksession.insert(factShoppingCartItem);

                    }

                    ksession.startProcess("com.redhat.coolstore.PriceProcess");

                    ksession.fireAllRules();

                }

                sc.setCartItemTotal(factShoppingCart.getCartItemTotal());
                sc.setCartItemPromoSavings(factShoppingCart.getCartItemPromoSavings());
                sc.setShippingTotal(factShoppingCart.getShippingTotal());
                sc.setShippingPromoSavings(factShoppingCart.getShippingPromoSavings());
                sc.setCartTotal(factShoppingCart.getCartTotal());

            } finally {

                if ( ksession != null ) {

                    ksession.dispose();

                }
            }
        }

    }
}